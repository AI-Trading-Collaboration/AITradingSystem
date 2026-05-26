from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from ai_trading_system.config import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATA_QUALITY_CONFIG_PATH,
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    PROJECT_ROOT,
    configured_rate_series,
    dedupe_preserving_order,
    load_data_quality,
    load_market_regimes,
    load_sec_companies,
    load_universe,
    market_regime_by_id,
)
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.fundamentals.sec_pit_backfill import SEC_PIT_BACKTEST_DATA_GRADE

SEC_PIT_EVALUATION_TASK_ID = "TRADING-040"
SEC_PIT_EVALUATION_PRODUCTION_EFFECT = "none"
SEC_PIT_EVALUATION_REPORT_TYPE = "sec_pit_cognitive_evaluation"
DEFAULT_SEC_PIT_EVALUATION_CONFIG_PATH = PROJECT_ROOT / "config" / "sec_pit_evaluation.yaml"
DEFAULT_SEC_PIT_EVALUATION_POLICY_PATH = DEFAULT_SEC_PIT_EVALUATION_CONFIG_PATH
DEFAULT_SEC_PIT_EVALUATION_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "sec_pit_evaluation"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
DEFAULT_SEC_PIT_FEATURE_PANEL_PATH = (
    PROJECT_ROOT / "data" / "processed" / "sec_edgar" / "sec_pit_feature_panel.csv"
)

FORWARD_RETURN_HORIZONS: tuple[int, ...] = (1, 5, 20, 60)
PRIMARY_FORWARD_RETURN_HORIZON = 20
SHADOW_RECOMMENDATIONS: tuple[str, ...] = (
    "PROMOTE_TO_SHADOW",
    "KEEP_RESEARCH_ONLY",
    "DOWNWEIGHT",
    "EXCLUDE_INSUFFICIENT_DATA",
)

FEATURE_EFFECTIVENESS_COLUMNS: tuple[str, ...] = (
    "feature_id",
    "metric_id",
    "sample_count",
    "coverage_ratio",
    "valid_ticker_count",
    "start_date",
    "end_date",
    "pit_grade",
    "ic_1d",
    "ic_5d",
    "ic_20d",
    "ic_60d",
    "rank_ic_20d",
    "hit_rate_20d",
    "avg_forward_return_top_quantile_20d",
    "avg_forward_return_bottom_quantile_20d",
    "spread_top_minus_bottom_20d",
    "max_drawdown_top_quantile_20d",
    "stability_score",
    "data_quality_score",
    "recommendation",
)

SIGNAL_ATTRIBUTION_COLUMNS: tuple[str, ...] = (
    "decision_date",
    "ticker",
    "feature_id",
    "metric_id",
    "feature_value",
    "normalized_value",
    "signal_direction",
    "weight",
    "contribution",
    "available_time",
    "period",
    "form",
    "accession_number",
    "pit_grade",
    "forward_return_20d",
    "relative_return_vs_QQQ_20d",
)

SHADOW_CANDIDATE_WEIGHT_COLUMNS: tuple[str, ...] = (
    "feature_id",
    "metric_id",
    "current_weight",
    "suggested_shadow_weight",
    "weight_delta",
    "evidence_score",
    "stability_score",
    "coverage_ratio",
    "pit_quality_score",
    "risk_note",
    "manual_review_required",
    "production_effect",
)


@dataclass(frozen=True)
class SecPitEvaluationPolicy:
    policy_version: str
    owner: str
    status: str
    rationale: str
    review_condition: str
    min_coverage_ratio: float
    min_valid_ticker_count: int
    min_sample_count: int
    min_abs_rank_ic_20d: float
    min_stability_score: float
    min_pit_quality_score: float
    winsorize_lower_quantile: float
    winsorize_upper_quantile: float
    top_quantile: float
    max_abs_shadow_weight: float
    pit_quality_weights: dict[str, float]


@dataclass(frozen=True)
class SecPitEvaluationArtifacts:
    status: str
    summary_json_path: Path
    summary_markdown_path: Path
    feature_effectiveness_path: Path
    signal_attribution_path: Path
    shadow_candidate_weights_path: Path
    data_quality_report_path: Path
    run_log_path: Path

    @property
    def json_path(self) -> Path:
        return self.summary_json_path

    @property
    def markdown_path(self) -> Path:
        return self.summary_markdown_path


def load_sec_pit_evaluation_policy(
    path: Path | str = DEFAULT_SEC_PIT_EVALUATION_CONFIG_PATH,
) -> SecPitEvaluationPolicy:
    raw_path = Path(path)
    with raw_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}
    section = raw.get("sec_pit_evaluation", raw)
    if not isinstance(section, dict):
        raise ValueError("sec_pit_evaluation policy must be a mapping")
    quality_weights = section.get("pit_quality_weights") or {}
    if not isinstance(quality_weights, dict):
        raise ValueError("sec_pit_evaluation.pit_quality_weights must be a mapping")
    policy = SecPitEvaluationPolicy(
        policy_version=str(section.get("policy_version", "sec_pit_evaluation.v1")),
        owner=str(section.get("owner", "system")),
        status=str(section.get("status", "pilot_baseline")),
        rationale=str(section.get("rationale", "")),
        review_condition=str(section.get("review_condition", "")),
        min_coverage_ratio=float(section.get("min_coverage_ratio", 0.6)),
        min_valid_ticker_count=int(section.get("min_valid_ticker_count", 5)),
        min_sample_count=int(section.get("min_sample_count", 30)),
        min_abs_rank_ic_20d=float(section.get("min_abs_rank_ic_20d", 0.03)),
        min_stability_score=float(section.get("min_stability_score", 0.5)),
        min_pit_quality_score=float(section.get("min_pit_quality_score", 0.8)),
        winsorize_lower_quantile=float(section.get("winsorize_lower_quantile", 0.01)),
        winsorize_upper_quantile=float(section.get("winsorize_upper_quantile", 0.99)),
        top_quantile=float(section.get("top_quantile", 0.20)),
        max_abs_shadow_weight=float(section.get("max_abs_shadow_weight", 0.05)),
        pit_quality_weights={str(key): float(value) for key, value in quality_weights.items()},
    )
    _validate_policy(policy)
    return policy


def run_sec_pit_evaluation(
    *,
    start: date,
    end: date,
    feature_panel_path: Path = DEFAULT_SEC_PIT_FEATURE_PANEL_PATH,
    universe_path: Path = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    benchmark: str = "QQQ",
    output_dir: Path = DEFAULT_SEC_PIT_EVALUATION_OUTPUT_DIR,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    market_universe_path: Path = DEFAULT_CONFIG_PATH,
    data_quality_config_path: Path = DEFAULT_DATA_QUALITY_CONFIG_PATH,
    quality_report_path: Path | None = None,
    quality_as_of: date | None = None,
    policy_path: Path = DEFAULT_SEC_PIT_EVALUATION_CONFIG_PATH,
    market_regimes_path: Path = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    regime: str | None = None,
    tickers: list[str] | None = None,
) -> SecPitEvaluationArtifacts:
    if start > end:
        raise ValueError("start must be on or before end")

    output_dir.mkdir(parents=True, exist_ok=True)
    benchmark = benchmark.upper()
    policy = load_sec_pit_evaluation_policy(policy_path)
    active_tickers = _resolve_tickers(universe_path=universe_path, tickers=tickers)
    market_universe = load_universe(market_universe_path)
    market_regimes = load_market_regimes(market_regimes_path)
    selected_regime = market_regime_by_id(
        market_regimes,
        regime or market_regimes.default_backtest_regime,
    )
    quality_date = quality_as_of or end
    quality_output = quality_report_path or default_quality_report_path(output_dir, quality_date)
    summary_date = end.isoformat()
    summary_json_path = output_dir / f"sec_pit_evaluation_summary_{summary_date}.json"
    summary_markdown_path = output_dir / f"sec_pit_evaluation_summary_{summary_date}.md"
    feature_effectiveness_path = output_dir / f"sec_pit_feature_effectiveness_{summary_date}.csv"
    signal_attribution_path = output_dir / f"sec_pit_signal_attribution_{summary_date}.csv"
    shadow_candidate_weights_path = (
        output_dir / f"sec_pit_shadow_candidate_weights_{summary_date}.csv"
    )
    run_log_path = output_dir / "run.log"

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=dedupe_preserving_order([*active_tickers, benchmark]),
        expected_rate_series=configured_rate_series(market_universe),
        quality_config=load_data_quality(data_quality_config_path),
        as_of=quality_date,
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        empty_effectiveness = pd.DataFrame(columns=list(FEATURE_EFFECTIVENESS_COLUMNS))
        empty_attribution = pd.DataFrame(columns=list(SIGNAL_ATTRIBUTION_COLUMNS))
        empty_weights = pd.DataFrame(columns=list(SHADOW_CANDIDATE_WEIGHT_COLUMNS))
        empty_effectiveness.to_csv(feature_effectiveness_path, index=False)
        empty_attribution.to_csv(signal_attribution_path, index=False)
        empty_weights.to_csv(shadow_candidate_weights_path, index=False)
        summary = _summary_payload(
            status="DATA_QUALITY_FAILED",
            start=start,
            end=end,
            feature_panel_path=feature_panel_path,
            universe_path=universe_path,
            benchmark=benchmark,
            selected_regime=selected_regime,
            policy=policy,
            data_quality_report=data_quality_report,
            data_quality_report_path=quality_output,
            active_tickers=active_tickers,
            feature_effectiveness=empty_effectiveness,
            coverage=_empty_coverage(),
            artifacts={
                "summary_json": summary_json_path,
                "summary_markdown": summary_markdown_path,
                "feature_effectiveness_csv": feature_effectiveness_path,
                "signal_attribution_csv": signal_attribution_path,
                "shadow_candidate_weights_csv": shadow_candidate_weights_path,
            },
            limitations=["cached market/macro data quality gate failed; evaluation stopped"],
        )
        _write_outputs(
            summary=summary,
            markdown=render_sec_pit_evaluation_summary(summary, empty_effectiveness),
            summary_json_path=summary_json_path,
            summary_markdown_path=summary_markdown_path,
            run_log_path=run_log_path,
            data_quality_report_path=quality_output,
        )
        return SecPitEvaluationArtifacts(
            status="DATA_QUALITY_FAILED",
            summary_json_path=summary_json_path,
            summary_markdown_path=summary_markdown_path,
            feature_effectiveness_path=feature_effectiveness_path,
            signal_attribution_path=signal_attribution_path,
            shadow_candidate_weights_path=shadow_candidate_weights_path,
            data_quality_report_path=quality_output,
            run_log_path=run_log_path,
        )

    raw_panel = _load_raw_feature_panel(feature_panel_path)
    normalized_panel = _normalize_feature_panel(raw_panel)
    prepared_panel, coverage = _prepare_feature_panel(
        normalized_panel,
        start=start,
        end=end,
        tickers=active_tickers,
        policy=policy,
    )
    prices = pd.read_csv(prices_path, dtype=str).fillna("")
    labels = _build_forward_labels(
        prices=prices,
        tickers=active_tickers,
        benchmark=benchmark,
        start=start,
        end=end,
    )
    evaluation_frame = _prepare_evaluation_frame(
        prepared_panel=prepared_panel,
        labels=labels,
        policy=policy,
    )
    feature_effectiveness = _feature_effectiveness(
        evaluation_frame=evaluation_frame,
        expected_tickers=active_tickers,
        start=start,
        end=end,
        policy=policy,
    )
    shadow_candidate_weights = _shadow_candidate_weights(feature_effectiveness, policy)
    signal_attribution = _signal_attribution(
        evaluation_frame=evaluation_frame,
        shadow_candidate_weights=shadow_candidate_weights,
    )

    feature_effectiveness.to_csv(feature_effectiveness_path, index=False)
    signal_attribution.to_csv(signal_attribution_path, index=False)
    shadow_candidate_weights.to_csv(shadow_candidate_weights_path, index=False)
    limitations = _evaluation_limitations(
        start=start,
        selected_regime=selected_regime,
        coverage=coverage,
        feature_effectiveness=feature_effectiveness,
    )
    status = "PASS" if coverage["pit_violation_count"] == 0 else "PASS_WITH_EXCLUSIONS"
    summary = _summary_payload(
        status=status,
        start=start,
        end=end,
        feature_panel_path=feature_panel_path,
        universe_path=universe_path,
        benchmark=benchmark,
        selected_regime=selected_regime,
        policy=policy,
        data_quality_report=data_quality_report,
        data_quality_report_path=quality_output,
        active_tickers=active_tickers,
        feature_effectiveness=feature_effectiveness,
        coverage=coverage,
        artifacts={
            "summary_json": summary_json_path,
            "summary_markdown": summary_markdown_path,
            "feature_effectiveness_csv": feature_effectiveness_path,
            "signal_attribution_csv": signal_attribution_path,
            "shadow_candidate_weights_csv": shadow_candidate_weights_path,
        },
        limitations=limitations,
    )
    _write_outputs(
        summary=summary,
        markdown=render_sec_pit_evaluation_summary(summary, feature_effectiveness),
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        run_log_path=run_log_path,
        data_quality_report_path=quality_output,
    )
    return SecPitEvaluationArtifacts(
        status=status,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        feature_effectiveness_path=feature_effectiveness_path,
        signal_attribution_path=signal_attribution_path,
        shadow_candidate_weights_path=shadow_candidate_weights_path,
        data_quality_report_path=quality_output,
        run_log_path=run_log_path,
    )


def render_sec_pit_evaluation_summary(
    summary: dict[str, Any],
    feature_effectiveness: pd.DataFrame | None = None,
) -> str:
    coverage = summary["data_coverage"]
    recommendations = summary["recommendations"]
    metadata = summary["metadata"]
    data_quality = summary["data_quality"]
    frame = (
        feature_effectiveness
        if feature_effectiveness is not None
        else pd.DataFrame(summary.get("top_features", []))
    )
    promote = (
        frame.loc[frame["recommendation"] == "PROMOTE_TO_SHADOW"].head(10)
        if not frame.empty and "recommendation" in frame.columns
        else pd.DataFrame()
    )
    research = (
        frame.loc[frame["recommendation"] == "KEEP_RESEARCH_ONLY"].head(10)
        if not frame.empty and "recommendation" in frame.columns
        else pd.DataFrame()
    )
    downweight = (
        frame.loc[frame["recommendation"] == "DOWNWEIGHT"].head(10)
        if not frame.empty and "recommendation" in frame.columns
        else pd.DataFrame()
    )
    insufficient = (
        frame.loc[frame["recommendation"] == "EXCLUDE_INSUFFICIENT_DATA"].head(10)
        if not frame.empty and "recommendation" in frame.columns
        else pd.DataFrame()
    )
    lines = [
        "# SEC PIT Cognitive Evaluation Summary",
        "",
        "## Metadata",
        f"- generated_at: {summary['generated_at']}",
        f"- start_date: {summary['start_date']}",
        f"- end_date: {summary['end_date']}",
        f"- universe: {metadata['universe_path']}",
        f"- universe_size: {summary['universe_size']}",
        f"- feature_panel_path: {metadata['feature_panel_path']}",
        f"- pit_grade policy: {summary['pit_grade_policy']}",
        f"- market_regime: {metadata['market_regime_id']} ({metadata['market_regime_name']})",
        f"- data_quality_status: {data_quality['status']}",
        f"- production_effect: {summary['production_effect']}",
        "",
        "## Data Coverage",
        f"- ticker coverage: {coverage.get('ticker_coverage_ratio', 0.0):.4f}",
        f"- feature coverage: {coverage.get('feature_coverage_ratio', 0.0):.4f}",
        f"- input_rows: {coverage['input_rows']}",
        f"- valid_rows: {coverage['valid_rows']}",
        f"- excluded_rows: {coverage['excluded_rows']}",
        f"- missing available_time count: {coverage['missing_available_time']}",
        f"- PIT violation count: {coverage['pit_violation_count']}",
        (
            "- B-grade reconstructed PIT ratio: "
            f"{coverage.get('b_grade_reconstructed_ratio', 0.0):.4f}"
        ),
        "",
        "## Feature Effectiveness",
    ]
    lines.extend(_feature_table_lines("top positive features", _top_positive(frame)))
    lines.extend(_feature_table_lines("top negative features", _top_negative(frame)))
    lines.extend(_feature_table_lines("unstable features", _unstable(frame)))
    lines.extend(_feature_table_lines("insufficient data features", insufficient))
    lines.extend(
        [
            "",
            "## Shadow Candidate Weights",
            f"- promote candidates: {recommendations['promote_to_shadow']}",
            f"- downweight candidates: {recommendations['downweight']}",
            f"- research-only candidates: {recommendations['keep_research_only']}",
        ]
    )
    lines.extend(_feature_table_lines("promote candidates", promote))
    lines.extend(_feature_table_lines("downweight candidates", downweight))
    lines.extend(_feature_table_lines("research-only candidates", research))
    lines.extend(
        [
            "",
            "## PIT Safety Checks",
            f"- violations: {coverage['pit_violation_count']}",
            f"- downgraded rows: {coverage.get('downgraded_rows', 0)}",
            f"- excluded rows: {coverage['excluded_rows']}",
            f"- missing provenance rows: {coverage.get('missing_provenance_rows', 0)}",
            "",
            "## Interpretation",
            "- 本报告评估 SEC reconstructed PIT fundamentals 的离线解释力；"
            "它不修改 production score 权重。",
            "- 与 price-only baseline 的差异需要和同区间 backtest / shadow outcome 报告一起复核；"
            "本报告只提供 feature-level evidence 和 shadow candidate 建议。",
            "- SEC PIT fundamentals 是否改善 signal quality 取决于 rank IC、top-bottom spread、"
            "coverage、PIT quality 和后续人工 review。",
        ]
    )
    for limitation in summary.get("limitations", []):
        lines.append(f"- limitation: {limitation}")
    lines.extend(
        [
            "",
            "## Manual Review Checklist",
            (
                "- 确认所有 promoted feature 的 accession_number / accepted_datetime / "
                "filed_date / raw_sha256 provenance。"
            ),
            "- 复核负向 rank IC feature 的经济含义，避免方向性误读。",
            "- 用独立 shadow outcome window 验证候选，不得直接写入 production weights。",
            "- 确认数据质量报告和 PIT safety exclusion count 可解释。",
        ]
    )
    return "\n".join(lines) + "\n"


def _validate_policy(policy: SecPitEvaluationPolicy) -> None:
    if policy.min_valid_ticker_count < 2:
        raise ValueError("min_valid_ticker_count must be at least 2")
    if policy.min_sample_count <= 0:
        raise ValueError("min_sample_count must be positive")
    for name, value in (
        ("min_coverage_ratio", policy.min_coverage_ratio),
        ("min_abs_rank_ic_20d", policy.min_abs_rank_ic_20d),
        ("min_stability_score", policy.min_stability_score),
        ("min_pit_quality_score", policy.min_pit_quality_score),
        ("winsorize_lower_quantile", policy.winsorize_lower_quantile),
        ("winsorize_upper_quantile", policy.winsorize_upper_quantile),
        ("top_quantile", policy.top_quantile),
        ("max_abs_shadow_weight", policy.max_abs_shadow_weight),
    ):
        if value < 0:
            raise ValueError(f"{name} must be non-negative")
    if not policy.winsorize_lower_quantile < policy.winsorize_upper_quantile:
        raise ValueError("winsorize_lower_quantile must be below winsorize_upper_quantile")
    if policy.winsorize_upper_quantile > 1:
        raise ValueError("winsorize_upper_quantile must be <= 1")
    if policy.top_quantile <= 0 or policy.top_quantile >= 0.5:
        raise ValueError("top_quantile must be in (0, 0.5)")
    if not policy.pit_quality_weights:
        raise ValueError("pit_quality_weights must not be empty")
    weight_sum = sum(policy.pit_quality_weights.values())
    if not np.isclose(weight_sum, 1.0):
        raise ValueError("pit_quality_weights must sum to 1.0")


def _resolve_tickers(*, universe_path: Path, tickers: list[str] | None) -> list[str]:
    if tickers:
        resolved = [item.upper() for item in _flatten_ticker_options(tickers) if item.strip()]
    else:
        sec_companies = load_sec_companies(universe_path)
        resolved = [company.ticker.upper() for company in sec_companies.companies if company.active]
    resolved = dedupe_preserving_order(resolved)
    if not resolved:
        raise ValueError("SEC PIT evaluation universe has no active tickers")
    return resolved


def _flatten_ticker_options(values: list[str]) -> list[str]:
    flattened: list[str] = []
    for value in values:
        flattened.extend(part.strip().upper() for part in str(value).replace(",", " ").split())
    return flattened


def _load_raw_feature_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"SEC PIT feature panel does not exist: {path}")
    return pd.read_csv(path, dtype=str).fillna("")


def _normalize_feature_panel(frame: pd.DataFrame) -> pd.DataFrame:
    required_any = {
        "decision_date": ("decision_date", "date", "signal_date"),
        "ticker": ("ticker",),
        "feature_id": ("feature_id",),
        "feature_value": ("feature_value", "value"),
    }
    missing = [
        canonical
        for canonical, choices in required_any.items()
        if not any(choice in frame.columns for choice in choices)
    ]
    if missing:
        raise ValueError(f"SEC PIT feature panel missing required columns: {', '.join(missing)}")
    normalized = pd.DataFrame(
        {
            "decision_date": _first_existing(frame, ("decision_date", "date", "signal_date")),
            "ticker": _first_existing(frame, ("ticker",)),
            "cik": _first_existing(frame, ("cik",)),
            "feature_id": _first_existing(frame, ("feature_id",)),
            "metric_id": _first_existing(frame, ("metric_id", "input_metric_ids")),
            "period": _first_existing(frame, ("period", "period_end", "period_type")),
            "available_time": _first_existing(
                frame,
                ("available_time", "max_input_available_time_utc", "available_time_utc"),
            ),
            "decision_time": _first_existing(frame, ("decision_time",)),
            "feature_value": _first_existing(frame, ("feature_value", "value")),
            "pit_grade": _first_existing(frame, ("pit_grade", "pit_data_grade")),
            "accession_number": _first_existing(
                frame,
                ("accession_number", "input_accession_numbers", "source_accession_number"),
            ),
            "form": _first_existing(frame, ("form",)),
            "source_concept": _first_existing(frame, ("source_concept", "input_metric_ids")),
            "raw_sha256": _first_existing(frame, ("raw_sha256",)),
            "accepted_datetime": _first_existing(frame, ("accepted_datetime",)),
            "filed_date": _first_existing(frame, ("filed_date",)),
        }
    ).fillna("")
    normalized["ticker"] = normalized["ticker"].astype(str).str.upper()
    normalized["feature_id"] = normalized["feature_id"].astype(str)
    normalized["metric_id"] = normalized["metric_id"].astype(str)
    return normalized


def _first_existing(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.Series:
    for column in columns:
        if column in frame.columns:
            return frame[column].astype(str)
    return pd.Series([""] * len(frame), index=frame.index, dtype="object")


def _prepare_feature_panel(
    frame: pd.DataFrame,
    *,
    start: date,
    end: date,
    tickers: list[str],
    policy: SecPitEvaluationPolicy,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    input_rows = len(frame)
    if frame.empty:
        return frame.copy(), _empty_coverage(input_rows=input_rows)
    requested = set(tickers)
    prepared = frame.copy()
    prepared["_decision_date"] = pd.to_datetime(prepared["decision_date"], errors="coerce")
    prepared["_available_time"] = pd.to_datetime(
        prepared["available_time"], errors="coerce", utc=True
    )
    prepared["_decision_time"] = _decision_time_series(prepared)
    prepared["_feature_value"] = pd.to_numeric(prepared["feature_value"], errors="coerce")
    prepared["_pit_quality_score"] = prepared.apply(
        lambda row: _row_pit_quality_score(row, policy),
        axis=1,
    )
    missing_available = prepared["_available_time"].isna()
    invalid_decision = prepared["_decision_date"].isna()
    future_violation = (
        prepared["_available_time"].notna()
        & prepared["_decision_time"].notna()
        & (prepared["_available_time"] > prepared["_decision_time"])
    )
    out_of_range = prepared["_decision_date"].notna() & (
        (prepared["_decision_date"].dt.date < start) | (prepared["_decision_date"].dt.date > end)
    )
    ticker_excluded = ~prepared["ticker"].isin(requested)
    non_numeric = prepared["_feature_value"].isna()
    missing_provenance = _missing_provenance_mask(prepared)
    valid_mask = (
        ~missing_available
        & ~invalid_decision
        & ~future_violation
        & ~out_of_range
        & ~ticker_excluded
        & ~non_numeric
    )
    valid = prepared.loc[valid_mask].copy()
    if not valid.empty:
        valid["decision_date"] = valid["_decision_date"].dt.date.astype(str)
        valid["feature_value"] = valid["_feature_value"].astype(float)
        valid["pit_quality_score"] = valid["_pit_quality_score"].astype(float)
    b_grade = prepared["pit_grade"].astype(str) == SEC_PIT_BACKTEST_DATA_GRADE
    expected_rows = len(tickers) * max(_calendar_day_count(start, end), 1)
    coverage = {
        "input_rows": int(input_rows),
        "valid_rows": int(len(valid)),
        "excluded_rows": int(input_rows - len(valid)),
        "missing_available_time": int(missing_available.sum()),
        "pit_violation_count": int(future_violation.sum()),
        "invalid_decision_date_count": int(invalid_decision.sum()),
        "out_of_range_rows": int(out_of_range.sum()),
        "ticker_excluded_rows": int(ticker_excluded.sum()),
        "non_numeric_feature_value_rows": int(non_numeric.sum()),
        "missing_provenance_rows": int(missing_provenance.sum()),
        "downgraded_rows": int(
            (prepared["_pit_quality_score"] < policy.min_pit_quality_score).sum()
        ),
        "b_grade_reconstructed_ratio": _safe_ratio(int(b_grade.sum()), input_rows),
        "ticker_coverage_ratio": _safe_ratio(valid["ticker"].nunique(), len(tickers)),
        "feature_coverage_ratio": _safe_ratio(len(valid), expected_rows),
    }
    return valid.reset_index(drop=True), coverage


def _decision_time_series(frame: pd.DataFrame) -> pd.Series:
    explicit = pd.to_datetime(frame["decision_time"], errors="coerce", utc=True)
    fallback = pd.to_datetime(
        frame["_decision_date"].dt.date.astype(str) + "T23:59:59+00:00",
        errors="coerce",
        utc=True,
    )
    return explicit.fillna(fallback)


def _row_pit_quality_score(row: pd.Series, policy: SecPitEvaluationPolicy) -> float:
    score = 0.0
    for field, weight in policy.pit_quality_weights.items():
        if field == "pit_grade":
            if str(row.get("pit_grade") or "") == SEC_PIT_BACKTEST_DATA_GRADE:
                score += weight
        elif str(row.get(field) or "").strip():
            score += weight
    return float(score)


def _missing_provenance_mask(frame: pd.DataFrame) -> pd.Series:
    required = ("accession_number", "accepted_datetime", "filed_date", "raw_sha256")
    mask = pd.Series(False, index=frame.index)
    for column in required:
        mask = mask | ~frame[column].astype(str).str.strip().astype(bool)
    return mask


def _build_forward_labels(
    *,
    prices: pd.DataFrame,
    tickers: list[str],
    benchmark: str,
    start: date,
    end: date,
) -> pd.DataFrame:
    required = {"date", "ticker", "adj_close"}
    missing = sorted(required - set(prices.columns))
    if missing:
        raise ValueError(f"prices CSV missing columns: {', '.join(missing)}")
    required_tickers = set(tickers) | {benchmark}
    frame = prices.loc[prices["ticker"].astype(str).str.upper().isin(required_tickers)].copy()
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].sort_values(
        ["ticker", "_date"]
    )
    if frame.empty:
        return _empty_labels()
    label_frames: list[pd.DataFrame] = []
    for _ticker, group in frame.groupby("ticker", sort=True):
        labels = group[["_date", "ticker", "_adj_close"]].copy()
        prices_array = labels["_adj_close"].to_numpy(dtype=float)
        for horizon in FORWARD_RETURN_HORIZONS:
            future = pd.Series(prices_array).shift(-horizon).to_numpy(dtype=float)
            labels[f"forward_return_{horizon}d"] = future / prices_array - 1.0
        labels[f"max_drawdown_forward_{PRIMARY_FORWARD_RETURN_HORIZON}d"] = _forward_max_drawdown(
            prices_array, PRIMARY_FORWARD_RETURN_HORIZON
        )
        label_frames.append(labels)
    merged = pd.concat(label_frames, ignore_index=True)
    selected = merged.loc[
        (merged["_date"].dt.date >= start)
        & (merged["_date"].dt.date <= end)
        & merged["ticker"].isin(tickers)
    ].copy()
    benchmark_returns = merged.loc[
        merged["ticker"] == benchmark,
        ["_date", f"forward_return_{PRIMARY_FORWARD_RETURN_HORIZON}d"],
    ].rename(
        columns={
            f"forward_return_{PRIMARY_FORWARD_RETURN_HORIZON}d": (
                f"benchmark_forward_return_{PRIMARY_FORWARD_RETURN_HORIZON}d"
            )
        }
    )
    selected = selected.merge(benchmark_returns, on="_date", how="left")
    selected[f"relative_return_vs_{benchmark}_{PRIMARY_FORWARD_RETURN_HORIZON}d"] = (
        selected[f"forward_return_{PRIMARY_FORWARD_RETURN_HORIZON}d"]
        - selected[f"benchmark_forward_return_{PRIMARY_FORWARD_RETURN_HORIZON}d"]
    )
    selected["decision_date"] = selected["_date"].dt.date.astype(str)
    return selected.drop(columns=["_date", "_adj_close"]).reset_index(drop=True)


def _empty_labels() -> pd.DataFrame:
    columns = ["decision_date", "ticker"]
    columns.extend(f"forward_return_{horizon}d" for horizon in FORWARD_RETURN_HORIZONS)
    columns.append(f"max_drawdown_forward_{PRIMARY_FORWARD_RETURN_HORIZON}d")
    columns.append(f"relative_return_vs_QQQ_{PRIMARY_FORWARD_RETURN_HORIZON}d")
    return pd.DataFrame(columns=columns)


def _forward_max_drawdown(values: np.ndarray, horizon: int) -> np.ndarray:
    results: list[float] = []
    for index, current in enumerate(values):
        future = values[index + 1 : index + horizon + 1]
        if len(future) == 0 or not np.isfinite(current) or current == 0:
            results.append(np.nan)
            continue
        results.append(float(np.nanmin(future / current - 1.0)))
    return np.array(results, dtype=float)


def _prepare_evaluation_frame(
    *,
    prepared_panel: pd.DataFrame,
    labels: pd.DataFrame,
    policy: SecPitEvaluationPolicy,
) -> pd.DataFrame:
    if prepared_panel.empty:
        return _empty_evaluation_frame()
    merged = prepared_panel.merge(labels, on=["decision_date", "ticker"], how="left")
    merged["normalized_value"] = np.nan
    for (_decision_date, _feature_id, _metric_id), index in merged.groupby(
        ["decision_date", "feature_id", "metric_id"], sort=False
    ).groups.items():
        values = merged.loc[index, "feature_value"].astype(float)
        normalized = _winsorized_zscore(values, policy)
        merged.loc[index, "normalized_value"] = normalized
    return merged.reset_index(drop=True)


def _empty_evaluation_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "decision_date",
            "ticker",
            "feature_id",
            "metric_id",
            "feature_value",
            "normalized_value",
            "available_time",
            "period",
            "form",
            "accession_number",
            "pit_grade",
            "pit_quality_score",
        ]
    )


def _winsorized_zscore(values: pd.Series, policy: SecPitEvaluationPolicy) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.notna().sum() < 2:
        return pd.Series([0.0] * len(values), index=values.index, dtype=float)
    lower = numeric.quantile(policy.winsorize_lower_quantile)
    upper = numeric.quantile(policy.winsorize_upper_quantile)
    clipped = numeric.clip(lower=lower, upper=upper)
    std = clipped.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series([0.0] * len(values), index=values.index, dtype=float)
    return (clipped - clipped.mean()) / std


def _feature_effectiveness(
    *,
    evaluation_frame: pd.DataFrame,
    expected_tickers: list[str],
    start: date,
    end: date,
    policy: SecPitEvaluationPolicy,
) -> pd.DataFrame:
    if evaluation_frame.empty:
        return pd.DataFrame(columns=list(FEATURE_EFFECTIVENESS_COLUMNS))
    records: list[dict[str, Any]] = []
    expected_row_count = len(expected_tickers) * max(_calendar_day_count(start, end), 1)
    for (feature_id, metric_id), group in evaluation_frame.groupby(
        ["feature_id", "metric_id"],
        sort=True,
    ):
        matched = group.loc[
            group["normalized_value"].notna()
            & group[f"forward_return_{PRIMARY_FORWARD_RETURN_HORIZON}d"].notna()
        ].copy()
        daily_ic = {
            horizon: _daily_ic_values(
                group,
                return_column=f"forward_return_{horizon}d",
                min_valid_ticker_count=policy.min_valid_ticker_count,
                rank=False,
            )
            for horizon in FORWARD_RETURN_HORIZONS
        }
        daily_rank_ic_20d = _daily_ic_values(
            group,
            return_column=f"forward_return_{PRIMARY_FORWARD_RETURN_HORIZON}d",
            min_valid_ticker_count=policy.min_valid_ticker_count,
            rank=True,
        )
        rank_ic_20d = _mean_or_nan(daily_rank_ic_20d)
        stability_score = _stability_score(rank_ic_20d, daily_rank_ic_20d)
        quantile = _quantile_spread(group, policy)
        coverage_ratio = _safe_ratio(len(group), expected_row_count)
        pit_grade = _dominant_value(group["pit_grade"])
        data_quality_score = float(group["pit_quality_score"].mean()) if len(group) else np.nan
        hit_rate = _hit_rate(matched, rank_ic_20d)
        recommendation = _recommendation(
            sample_count=len(matched),
            coverage_ratio=coverage_ratio,
            rank_ic_20d=rank_ic_20d,
            stability_score=stability_score,
            pit_quality_score=data_quality_score,
            valid_ticker_count=int(matched["ticker"].nunique()),
            policy=policy,
        )
        records.append(
            {
                "feature_id": str(feature_id),
                "metric_id": str(metric_id),
                "sample_count": int(len(matched)),
                "coverage_ratio": coverage_ratio,
                "valid_ticker_count": int(matched["ticker"].nunique()),
                "start_date": _min_date(matched["decision_date"]),
                "end_date": _max_date(matched["decision_date"]),
                "pit_grade": pit_grade,
                "ic_1d": _mean_or_nan(daily_ic[1]),
                "ic_5d": _mean_or_nan(daily_ic[5]),
                "ic_20d": _mean_or_nan(daily_ic[20]),
                "ic_60d": _mean_or_nan(daily_ic[60]),
                "rank_ic_20d": rank_ic_20d,
                "hit_rate_20d": hit_rate,
                "avg_forward_return_top_quantile_20d": quantile["top"],
                "avg_forward_return_bottom_quantile_20d": quantile["bottom"],
                "spread_top_minus_bottom_20d": quantile["spread"],
                "max_drawdown_top_quantile_20d": quantile["max_drawdown_top"],
                "stability_score": stability_score,
                "data_quality_score": data_quality_score,
                "recommendation": recommendation,
            }
        )
    frame = pd.DataFrame(records, columns=list(FEATURE_EFFECTIVENESS_COLUMNS))
    return frame.sort_values(
        ["recommendation", "rank_ic_20d", "feature_id", "metric_id"],
        ascending=[True, False, True, True],
        na_position="last",
    ).reset_index(drop=True)


def _daily_ic_values(
    frame: pd.DataFrame,
    *,
    return_column: str,
    min_valid_ticker_count: int,
    rank: bool,
) -> list[float]:
    if return_column not in frame.columns:
        return []
    values: list[float] = []
    for _decision_date, group in frame.groupby("decision_date", sort=True):
        clean = group.loc[
            group["normalized_value"].notna()
            & group[return_column].notna()
            & group["ticker"].notna()
        ].copy()
        if len(clean) < min_valid_ticker_count:
            continue
        feature = clean["normalized_value"].astype(float)
        returns = clean[return_column].astype(float)
        if feature.nunique() < 2 or returns.nunique() < 2:
            continue
        if rank:
            feature = feature.rank(method="average")
            returns = returns.rank(method="average")
        ic_value = feature.corr(returns)
        if pd.notna(ic_value):
            values.append(float(ic_value))
    return values


def _quantile_spread(group: pd.DataFrame, policy: SecPitEvaluationPolicy) -> dict[str, float]:
    top_returns: list[float] = []
    bottom_returns: list[float] = []
    spreads: list[float] = []
    drawdowns: list[float] = []
    return_column = f"forward_return_{PRIMARY_FORWARD_RETURN_HORIZON}d"
    drawdown_column = f"max_drawdown_forward_{PRIMARY_FORWARD_RETURN_HORIZON}d"
    for _decision_date, day in group.groupby("decision_date", sort=True):
        clean = day.loc[
            day["normalized_value"].notna() & day[return_column].notna() & day["ticker"].notna()
        ].copy()
        if len(clean) < policy.min_valid_ticker_count:
            continue
        clean = clean.sort_values("normalized_value")
        bucket_size = max(1, int(np.ceil(len(clean) * policy.top_quantile)))
        bottom = clean.head(bucket_size)
        top = clean.tail(bucket_size)
        top_return = float(top[return_column].mean())
        bottom_return = float(bottom[return_column].mean())
        top_returns.append(top_return)
        bottom_returns.append(bottom_return)
        spreads.append(top_return - bottom_return)
        if drawdown_column in top.columns and top[drawdown_column].notna().any():
            drawdowns.append(float(top[drawdown_column].mean()))
    return {
        "top": _mean_or_nan(top_returns),
        "bottom": _mean_or_nan(bottom_returns),
        "spread": _mean_or_nan(spreads),
        "max_drawdown_top": _mean_or_nan(drawdowns),
    }


def _hit_rate(frame: pd.DataFrame, rank_ic_20d: float) -> float:
    if frame.empty or pd.isna(rank_ic_20d):
        return np.nan
    direction = 1.0 if rank_ic_20d >= 0 else -1.0
    signals = frame["normalized_value"].astype(float) * direction
    returns = frame[f"forward_return_{PRIMARY_FORWARD_RETURN_HORIZON}d"].astype(float)
    usable = signals.notna() & returns.notna() & (signals != 0) & (returns != 0)
    if not usable.any():
        return np.nan
    return float(((signals.loc[usable] > 0) == (returns.loc[usable] > 0)).mean())


def _recommendation(
    *,
    sample_count: int,
    coverage_ratio: float,
    rank_ic_20d: float,
    stability_score: float,
    pit_quality_score: float,
    valid_ticker_count: int,
    policy: SecPitEvaluationPolicy,
) -> str:
    if (
        sample_count < policy.min_sample_count
        or coverage_ratio < policy.min_coverage_ratio
        or valid_ticker_count < policy.min_valid_ticker_count
    ):
        return "EXCLUDE_INSUFFICIENT_DATA"
    if pd.isna(rank_ic_20d) or pd.isna(stability_score) or pd.isna(pit_quality_score):
        return "KEEP_RESEARCH_ONLY"
    if (
        abs(rank_ic_20d) >= policy.min_abs_rank_ic_20d
        and stability_score >= policy.min_stability_score
        and pit_quality_score >= policy.min_pit_quality_score
    ):
        return "PROMOTE_TO_SHADOW"
    if (
        stability_score < policy.min_stability_score
        or pit_quality_score < policy.min_pit_quality_score
    ):
        return "KEEP_RESEARCH_ONLY"
    return "DOWNWEIGHT"


def _shadow_candidate_weights(
    feature_effectiveness: pd.DataFrame,
    policy: SecPitEvaluationPolicy,
) -> pd.DataFrame:
    if feature_effectiveness.empty:
        return pd.DataFrame(columns=list(SHADOW_CANDIDATE_WEIGHT_COLUMNS))
    records: list[dict[str, Any]] = []
    for row in feature_effectiveness.to_dict(orient="records"):
        rank_ic = _float_value(row.get("rank_ic_20d"))
        evidence_score = _evidence_score(row, policy)
        direction = -1.0 if pd.notna(rank_ic) and rank_ic < 0 else 1.0
        suggested = (
            direction * policy.max_abs_shadow_weight * evidence_score
            if row.get("recommendation") == "PROMOTE_TO_SHADOW"
            else 0.0
        )
        records.append(
            {
                "feature_id": row["feature_id"],
                "metric_id": row["metric_id"],
                "current_weight": 0.0,
                "suggested_shadow_weight": suggested,
                "weight_delta": suggested,
                "evidence_score": evidence_score,
                "stability_score": _float_value(row.get("stability_score")),
                "coverage_ratio": _float_value(row.get("coverage_ratio")),
                "pit_quality_score": _float_value(row.get("data_quality_score")),
                "risk_note": _risk_note(str(row.get("recommendation") or "")),
                "manual_review_required": True,
                "production_effect": SEC_PIT_EVALUATION_PRODUCTION_EFFECT,
            }
        )
    return pd.DataFrame(records, columns=list(SHADOW_CANDIDATE_WEIGHT_COLUMNS))


def _evidence_score(row: dict[str, Any], policy: SecPitEvaluationPolicy) -> float:
    ratios = [
        _threshold_ratio(_float_value(row.get("coverage_ratio")), policy.min_coverage_ratio),
        _threshold_ratio(float(row.get("sample_count") or 0), float(policy.min_sample_count)),
        _threshold_ratio(abs(_float_value(row.get("rank_ic_20d"))), policy.min_abs_rank_ic_20d),
        _threshold_ratio(_float_value(row.get("stability_score")), policy.min_stability_score),
        _threshold_ratio(_float_value(row.get("data_quality_score")), policy.min_pit_quality_score),
    ]
    clean = [value for value in ratios if pd.notna(value)]
    return float(np.mean(clean)) if clean else 0.0


def _threshold_ratio(value: float, threshold: float) -> float:
    if pd.isna(value):
        return np.nan
    if threshold <= 0:
        return 1.0
    return float(max(0.0, min(value / threshold, 1.0)))


def _risk_note(recommendation: str) -> str:
    if recommendation == "PROMOTE_TO_SHADOW":
        return "仅可进入 shadow candidate；manual review 后仍不得直接修改 production 权重。"
    if recommendation == "EXCLUDE_INSUFFICIENT_DATA":
        return "样本或覆盖不足；不得解释为有效基本面信号。"
    if recommendation == "KEEP_RESEARCH_ONLY":
        return "保留 research-only；需补 PIT provenance、样本或稳定性验证。"
    return "证据未达到 shadow promotion policy；建议降权或继续观察。"


def _signal_attribution(
    *,
    evaluation_frame: pd.DataFrame,
    shadow_candidate_weights: pd.DataFrame,
) -> pd.DataFrame:
    if evaluation_frame.empty:
        return pd.DataFrame(columns=list(SIGNAL_ATTRIBUTION_COLUMNS))
    weights = shadow_candidate_weights.set_index(["feature_id", "metric_id"])
    records: list[dict[str, Any]] = []
    for row in evaluation_frame.to_dict(orient="records"):
        key = (str(row.get("feature_id") or ""), str(row.get("metric_id") or ""))
        weight = 0.0
        direction = "UNKNOWN"
        if key in weights.index:
            weight = float(weights.loc[key, "suggested_shadow_weight"])
            if weight > 0:
                direction = "POSITIVE"
            elif weight < 0:
                direction = "NEGATIVE"
        normalized = _float_value(row.get("normalized_value"))
        contribution = normalized * weight if pd.notna(normalized) else np.nan
        records.append(
            {
                "decision_date": row.get("decision_date", ""),
                "ticker": row.get("ticker", ""),
                "feature_id": row.get("feature_id", ""),
                "metric_id": row.get("metric_id", ""),
                "feature_value": _float_value(row.get("feature_value")),
                "normalized_value": normalized,
                "signal_direction": direction,
                "weight": weight,
                "contribution": contribution,
                "available_time": row.get("available_time", ""),
                "period": row.get("period", ""),
                "form": row.get("form", ""),
                "accession_number": row.get("accession_number", ""),
                "pit_grade": row.get("pit_grade", ""),
                "forward_return_20d": _float_value(
                    row.get(f"forward_return_{PRIMARY_FORWARD_RETURN_HORIZON}d")
                ),
                "relative_return_vs_QQQ_20d": _float_value(
                    row.get(f"relative_return_vs_QQQ_{PRIMARY_FORWARD_RETURN_HORIZON}d")
                ),
            }
        )
    return pd.DataFrame(records, columns=list(SIGNAL_ATTRIBUTION_COLUMNS)).sort_values(
        ["decision_date", "ticker", "feature_id", "metric_id"]
    )


def _summary_payload(
    *,
    status: str,
    start: date,
    end: date,
    feature_panel_path: Path,
    universe_path: Path,
    benchmark: str,
    selected_regime: Any,
    policy: SecPitEvaluationPolicy,
    data_quality_report: DataQualityReport,
    data_quality_report_path: Path,
    active_tickers: list[str],
    feature_effectiveness: pd.DataFrame,
    coverage: dict[str, Any],
    artifacts: dict[str, Path],
    limitations: list[str],
) -> dict[str, Any]:
    recommendation_counts = Counter(feature_effectiveness["recommendation"].astype(str))
    return {
        "schema_version": "1.0",
        "report_type": SEC_PIT_EVALUATION_REPORT_TYPE,
        "task_id": SEC_PIT_EVALUATION_TASK_ID,
        "status": status,
        "production_effect": SEC_PIT_EVALUATION_PRODUCTION_EFFECT,
        "manual_review_required": True,
        "generated_at": datetime.now(UTC).isoformat(),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "universe_size": len(active_tickers),
        "feature_count": int(len(feature_effectiveness)),
        "pit_grade_policy": SEC_PIT_BACKTEST_DATA_GRADE,
        "metadata": {
            "feature_panel_path": str(feature_panel_path),
            "feature_panel_sha256": (
                _file_sha256(feature_panel_path) if feature_panel_path.exists() else ""
            ),
            "universe_path": str(universe_path),
            "benchmark": benchmark,
            "market_regime_id": selected_regime.regime_id,
            "market_regime_name": selected_regime.name,
            "market_regime_start_date": selected_regime.start_date.isoformat(),
            "market_regime_anchor_date": selected_regime.anchor_date.isoformat(),
            "market_regime_anchor_event": selected_regime.anchor_event,
            "tickers": active_tickers,
        },
        "policy": _policy_payload(policy),
        "data_quality": _data_quality_payload(data_quality_report, data_quality_report_path),
        "data_coverage": coverage,
        "recommendations": {
            "promote_to_shadow": int(recommendation_counts.get("PROMOTE_TO_SHADOW", 0)),
            "keep_research_only": int(recommendation_counts.get("KEEP_RESEARCH_ONLY", 0)),
            "downweight": int(recommendation_counts.get("DOWNWEIGHT", 0)),
            "exclude_insufficient_data": int(
                recommendation_counts.get("EXCLUDE_INSUFFICIENT_DATA", 0)
            ),
        },
        "top_features": _top_features_payload(feature_effectiveness),
        "limitations": limitations,
        "output_artifacts": {key: str(value) for key, value in artifacts.items()},
        "safety": {
            "available_time_gate": "available_time <= decision_time",
            "period_end_gate_allowed": False,
            "strict_forward_pit": False,
            "pit_grade": SEC_PIT_BACKTEST_DATA_GRADE,
            "manual_review_required": True,
            "production_effect": SEC_PIT_EVALUATION_PRODUCTION_EFFECT,
        },
    }


def _policy_payload(policy: SecPitEvaluationPolicy) -> dict[str, Any]:
    return {
        "policy_version": policy.policy_version,
        "owner": policy.owner,
        "status": policy.status,
        "rationale": policy.rationale,
        "review_condition": policy.review_condition,
        "min_coverage_ratio": policy.min_coverage_ratio,
        "min_valid_ticker_count": policy.min_valid_ticker_count,
        "min_sample_count": policy.min_sample_count,
        "min_abs_rank_ic_20d": policy.min_abs_rank_ic_20d,
        "min_stability_score": policy.min_stability_score,
        "min_pit_quality_score": policy.min_pit_quality_score,
        "winsorize_lower_quantile": policy.winsorize_lower_quantile,
        "winsorize_upper_quantile": policy.winsorize_upper_quantile,
        "top_quantile": policy.top_quantile,
        "max_abs_shadow_weight": policy.max_abs_shadow_weight,
        "pit_quality_weights": dict(policy.pit_quality_weights),
    }


def _data_quality_payload(report: DataQualityReport, report_path: Path) -> dict[str, Any]:
    return {
        "status": report.status,
        "report_path": str(report_path),
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
    }


def _top_features_payload(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    payload_frame = frame.copy()
    payload_frame["_abs_rank_ic_20d"] = payload_frame["rank_ic_20d"].abs()
    selected = payload_frame.sort_values(
        ["_abs_rank_ic_20d", "coverage_ratio", "feature_id"],
        ascending=[False, False, True],
        na_position="last",
    ).head(10)
    return [
        {key: _json_value(value) for key, value in record.items() if key != "_abs_rank_ic_20d"}
        for record in selected.to_dict(orient="records")
    ]


def _evaluation_limitations(
    *,
    start: date,
    selected_regime: Any,
    coverage: dict[str, Any],
    feature_effectiveness: pd.DataFrame,
) -> list[str]:
    limitations = [
        "SEC reconstructed PIT is B-grade filing-time PIT, not strict vendor archive PIT.",
        "Shadow candidate weights are observe-only and require manual review.",
    ]
    if start < selected_regime.start_date:
        limitations.append(
            "requested start date is before ai_after_chatgpt regime start; "
            "pre-regime rows are comparison/stress samples only"
        )
    if coverage.get("pit_violation_count", 0):
        limitations.append("rows with available_time after decision_time were excluded")
    if coverage.get("missing_available_time", 0):
        limitations.append("rows missing available_time were excluded from the main evaluation")
    if coverage.get("missing_provenance_rows", 0):
        limitations.append("rows missing SEC provenance were downgraded in pit_quality_score")
    if feature_effectiveness.empty:
        limitations.append("no feature had enough valid rows to produce effectiveness evidence")
    return limitations


def _write_outputs(
    *,
    summary: dict[str, Any],
    markdown: str,
    summary_json_path: Path,
    summary_markdown_path: Path,
    run_log_path: Path,
    data_quality_report_path: Path,
) -> None:
    summary_json_path.write_text(
        json.dumps(_json_value(summary), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    summary_markdown_path.write_text(markdown, encoding="utf-8")
    lines = [
        f"generated_at={datetime.now(UTC).isoformat()}",
        f"status={summary['status']}",
        f"summary_json={summary_json_path}",
        f"summary_markdown={summary_markdown_path}",
        f"data_quality_report={data_quality_report_path}",
        f"production_effect={SEC_PIT_EVALUATION_PRODUCTION_EFFECT}",
        f"pit_grade={SEC_PIT_BACKTEST_DATA_GRADE}",
    ]
    for name, value in summary.get("output_artifacts", {}).items():
        lines.append(f"{name}={value}")
    run_log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _empty_coverage(input_rows: int = 0) -> dict[str, Any]:
    return {
        "input_rows": input_rows,
        "valid_rows": 0,
        "excluded_rows": input_rows,
        "missing_available_time": 0,
        "pit_violation_count": 0,
        "invalid_decision_date_count": 0,
        "out_of_range_rows": 0,
        "ticker_excluded_rows": 0,
        "non_numeric_feature_value_rows": 0,
        "missing_provenance_rows": 0,
        "downgraded_rows": 0,
        "b_grade_reconstructed_ratio": 0.0,
        "ticker_coverage_ratio": 0.0,
        "feature_coverage_ratio": 0.0,
    }


def _feature_table_lines(title: str, frame: pd.DataFrame) -> list[str]:
    lines = ["", f"### {title}"]
    if frame.empty:
        lines.append("- none")
        return lines
    lines.extend(
        [
            "| feature_id | metric_id | rank_ic_20d | spread_20d | coverage | recommendation |",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for row in frame.head(10).to_dict(orient="records"):
        lines.append(
            "| "
            f"`{_escape_markdown(str(row.get('feature_id', '')))}` | "
            f"`{_escape_markdown(str(row.get('metric_id', '')))}` | "
            f"{_format_float(row.get('rank_ic_20d'))} | "
            f"{_format_float(row.get('spread_top_minus_bottom_20d'))} | "
            f"{_format_float(row.get('coverage_ratio'))} | "
            f"`{_escape_markdown(str(row.get('recommendation', '')))}` |"
        )
    return lines


def _top_positive(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "rank_ic_20d" not in frame.columns:
        return pd.DataFrame()
    return (
        frame.loc[frame["rank_ic_20d"] > 0]
        .sort_values(
            "rank_ic_20d",
            ascending=False,
        )
        .head(10)
    )


def _top_negative(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "rank_ic_20d" not in frame.columns:
        return pd.DataFrame()
    return frame.loc[frame["rank_ic_20d"] < 0].sort_values("rank_ic_20d").head(10)


def _unstable(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "recommendation" not in frame.columns:
        return pd.DataFrame()
    return frame.loc[
        (frame["recommendation"] == "KEEP_RESEARCH_ONLY")
        & (frame["stability_score"].fillna(0.0) < 0.5)
    ].head(10)


def _dominant_value(series: pd.Series) -> str:
    clean = [str(value) for value in series.dropna().tolist() if str(value)]
    if not clean:
        return ""
    return Counter(clean).most_common(1)[0][0]


def _min_date(series: pd.Series) -> str:
    if series.empty:
        return ""
    values = pd.to_datetime(series, errors="coerce").dropna()
    return values.min().date().isoformat() if not values.empty else ""


def _max_date(series: pd.Series) -> str:
    if series.empty:
        return ""
    values = pd.to_datetime(series, errors="coerce").dropna()
    return values.max().date().isoformat() if not values.empty else ""


def _mean_or_nan(values: list[float]) -> float:
    if not values:
        return np.nan
    return float(np.nanmean(values))


def _stability_score(mean_rank_ic: float, daily_values: list[float]) -> float:
    if not daily_values or pd.isna(mean_rank_ic) or mean_rank_ic == 0:
        return 0.0
    expected_positive = mean_rank_ic > 0
    same_sign = [
        value
        for value in daily_values
        if (value > 0 and expected_positive) or (value < 0 and not expected_positive)
    ]
    return _safe_ratio(len(same_sign), len(daily_values))


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def _calendar_day_count(start: date, end: date) -> int:
    return (end - start).days + 1


def _float_value(value: object) -> float:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return np.nan
    return result


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
    number = _float_value(value)
    if pd.isna(number):
        return ""
    return f"{number:.4f}"


def _escape_markdown(value: str) -> str:
    return value.replace("|", "\\|")


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
