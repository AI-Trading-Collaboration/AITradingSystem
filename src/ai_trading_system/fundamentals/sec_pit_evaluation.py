from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

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
from ai_trading_system.fundamentals.sec_pit_panel import SEC_PIT_FEATURE_PANEL_COLUMNS

SEC_PIT_EVALUATION_TASK_ID = "TRADING-040"
SEC_PIT_EVALUATION_PRODUCTION_EFFECT = "none"
DEFAULT_SEC_PIT_EVALUATION_POLICY_PATH = PROJECT_ROOT / "config" / "sec_pit_evaluation_policy.yaml"
DEFAULT_SEC_PIT_EVALUATION_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "sec_pit_evaluation"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
SEC_PIT_FEATURE_CONTRIBUTION_COLUMNS = (
    "feature_id",
    "period_type",
    "classification",
    "classification_reason",
    "recommended_action",
    "observation_count",
    "matched_return_observation_count",
    "unique_ticker_count",
    "unique_decision_day_count",
    "ticker_coverage_rate",
    "decision_day_coverage_rate",
    "mean_daily_rank_ic",
    "rank_ic_day_count",
    "monthly_rank_ic_count",
    "rank_ic_sign_stability",
    "top_minus_bottom_forward_excess_return",
    "direction_adjusted_top_bottom_forward_excess_return",
    "feature_value_unique_count",
    "pit_safety_issue_count",
)


@dataclass(frozen=True)
class SecPitEvaluationPolicy:
    policy_version: str
    owner: str
    status: str
    rationale: str
    review_condition: str
    forward_return_trading_days: int
    min_cross_sectional_tickers: int
    research_only_min_observations: int
    shadow_candidate_min_observations: int
    shadow_candidate_min_months: int
    shadow_candidate_min_ticker_coverage: float
    shadow_candidate_min_decision_day_coverage: float
    shadow_candidate_min_abs_mean_rank_ic: float
    shadow_candidate_min_rank_ic_sign_stability: float
    shadow_candidate_min_directional_spread: float
    top_bottom_quantile: float
    min_shadow_candidate_count_for_positive_loop: int


@dataclass(frozen=True)
class SecPitEvaluationArtifacts:
    status: str
    json_path: Path
    markdown_path: Path
    feature_contributions_path: Path | None
    shadow_candidates_path: Path | None
    data_quality_report_path: Path
    run_log_path: Path


def load_sec_pit_evaluation_policy(
    path: Path | str = DEFAULT_SEC_PIT_EVALUATION_POLICY_PATH,
) -> SecPitEvaluationPolicy:
    raw_path = Path(path)
    with raw_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}
    section = raw.get("sec_pit_evaluation", raw)
    if not isinstance(section, dict):
        raise ValueError("sec_pit_evaluation policy must be a mapping")
    return SecPitEvaluationPolicy(
        policy_version=str(section.get("policy_version", "sec_pit_evaluation.v1")),
        owner=str(section.get("owner", "system")),
        status=str(section.get("status", "pilot_baseline")),
        rationale=str(section.get("rationale", "")),
        review_condition=str(section.get("review_condition", "")),
        forward_return_trading_days=int(section.get("forward_return_trading_days", 1)),
        min_cross_sectional_tickers=int(section.get("min_cross_sectional_tickers", 3)),
        research_only_min_observations=int(section.get("research_only_min_observations", 10)),
        shadow_candidate_min_observations=int(section.get("shadow_candidate_min_observations", 60)),
        shadow_candidate_min_months=int(section.get("shadow_candidate_min_months", 3)),
        shadow_candidate_min_ticker_coverage=float(
            section.get("shadow_candidate_min_ticker_coverage", 0.50)
        ),
        shadow_candidate_min_decision_day_coverage=float(
            section.get("shadow_candidate_min_decision_day_coverage", 0.50)
        ),
        shadow_candidate_min_abs_mean_rank_ic=float(
            section.get("shadow_candidate_min_abs_mean_rank_ic", 0.03)
        ),
        shadow_candidate_min_rank_ic_sign_stability=float(
            section.get("shadow_candidate_min_rank_ic_sign_stability", 0.55)
        ),
        shadow_candidate_min_directional_spread=float(
            section.get("shadow_candidate_min_directional_spread", 0.0)
        ),
        top_bottom_quantile=float(section.get("top_bottom_quantile", 0.33)),
        min_shadow_candidate_count_for_positive_loop=int(
            section.get("min_shadow_candidate_count_for_positive_loop", 1)
        ),
    )


def run_sec_pit_evaluation(
    *,
    start: date,
    end: date,
    feature_panel_path: Path,
    universe_path: Path = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    benchmark: str = "QQQ",
    output_dir: Path = DEFAULT_SEC_PIT_EVALUATION_OUTPUT_DIR,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    market_universe_path: Path = DEFAULT_CONFIG_PATH,
    data_quality_config_path: Path = DEFAULT_DATA_QUALITY_CONFIG_PATH,
    quality_report_path: Path | None = None,
    quality_as_of: date | None = None,
    policy_path: Path = DEFAULT_SEC_PIT_EVALUATION_POLICY_PATH,
    market_regimes_path: Path = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    regime: str | None = None,
) -> SecPitEvaluationArtifacts:
    if start > end:
        raise ValueError("start must be on or before end")

    output_dir.mkdir(parents=True, exist_ok=True)
    benchmark = benchmark.upper()
    policy = load_sec_pit_evaluation_policy(policy_path)
    _validate_policy(policy)
    sec_companies = load_sec_companies(universe_path)
    tickers = [company.ticker for company in sec_companies.companies if company.active]
    if not tickers:
        raise ValueError("SEC PIT evaluation universe has no active companies")
    market_universe = load_universe(market_universe_path)
    market_regimes = load_market_regimes(market_regimes_path)
    selected_regime = market_regime_by_id(
        market_regimes,
        regime or market_regimes.default_backtest_regime,
    )
    quality_date = quality_as_of or end
    quality_output = quality_report_path or default_quality_report_path(output_dir, quality_date)
    json_path = output_dir / f"sec_pit_evaluation_{start.isoformat()}_{end.isoformat()}.json"
    markdown_path = output_dir / f"sec_pit_evaluation_{start.isoformat()}_{end.isoformat()}.md"
    feature_contributions_path = (
        output_dir / f"sec_pit_feature_contributions_{start.isoformat()}_{end.isoformat()}.csv"
    )
    shadow_candidates_path = (
        output_dir / f"sec_pit_shadow_candidates_{start.isoformat()}_{end.isoformat()}.csv"
    )
    run_log_path = output_dir / "run.log"

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=dedupe_preserving_order([*tickers, benchmark]),
        expected_rate_series=configured_rate_series(market_universe),
        quality_config=load_data_quality(data_quality_config_path),
        as_of=quality_date,
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        payload = _blocked_payload(
            status="DATA_QUALITY_FAILED",
            start=start,
            end=end,
            feature_panel_path=feature_panel_path,
            universe_path=universe_path,
            benchmark=benchmark,
            policy=policy,
            selected_regime=selected_regime,
            data_quality_report=data_quality_report,
            data_quality_report_path=quality_output,
            blocking_reason="cached market/macro data quality gate failed",
        )
        _write_blocked_outputs(
            payload=payload,
            json_path=json_path,
            markdown_path=markdown_path,
            run_log_path=run_log_path,
        )
        return SecPitEvaluationArtifacts(
            status="DATA_QUALITY_FAILED",
            json_path=json_path,
            markdown_path=markdown_path,
            feature_contributions_path=None,
            shadow_candidates_path=None,
            data_quality_report_path=quality_output,
            run_log_path=run_log_path,
        )

    feature_panel = _load_feature_panel(feature_panel_path)
    safety = _pit_safety_summary(feature_panel)
    if safety["future_available_time_violation_count"] > 0:
        payload = _blocked_payload(
            status="SAFETY_BLOCKED",
            start=start,
            end=end,
            feature_panel_path=feature_panel_path,
            universe_path=universe_path,
            benchmark=benchmark,
            policy=policy,
            selected_regime=selected_regime,
            data_quality_report=data_quality_report,
            data_quality_report_path=quality_output,
            blocking_reason="SEC PIT feature panel contains future availability leakage",
            pit_safety=safety,
        )
        _write_blocked_outputs(
            payload=payload,
            json_path=json_path,
            markdown_path=markdown_path,
            run_log_path=run_log_path,
        )
        return SecPitEvaluationArtifacts(
            status="SAFETY_BLOCKED",
            json_path=json_path,
            markdown_path=markdown_path,
            feature_contributions_path=None,
            shadow_candidates_path=None,
            data_quality_report_path=quality_output,
            run_log_path=run_log_path,
        )

    prices = pd.read_csv(prices_path, dtype=str).fillna("")
    returns = _build_forward_excess_returns(
        prices=prices,
        tickers=tickers,
        benchmark=benchmark,
        start=start,
        end=end,
        horizon_days=policy.forward_return_trading_days,
    )
    prepared = _prepare_feature_return_frame(
        feature_panel=feature_panel,
        returns=returns,
        tickers=tickers,
        start=start,
        end=end,
    )
    signal_dates = tuple(sorted(returns["decision_date"].unique()))
    contributions = _feature_contributions(
        prepared=prepared,
        tickers=tickers,
        signal_date_count=len(signal_dates),
        policy=policy,
    )
    contributions.to_csv(feature_contributions_path, index=False)
    shadow_candidates = contributions.loc[
        contributions["classification"].astype(str) == "SHADOW_CANDIDATE"
    ].copy()
    shadow_candidates.to_csv(shadow_candidates_path, index=False)

    payload = _evaluation_payload(
        start=start,
        end=end,
        feature_panel_path=feature_panel_path,
        universe_path=universe_path,
        benchmark=benchmark,
        policy=policy,
        selected_regime=selected_regime,
        data_quality_report=data_quality_report,
        data_quality_report_path=quality_output,
        pit_safety=safety,
        contributions=contributions,
        signal_date_count=len(signal_dates),
        active_tickers=tickers,
        matched_feature_return_rows=len(prepared.loc[prepared["forward_excess_return"].notna()]),
        feature_contributions_path=feature_contributions_path,
        shadow_candidates_path=shadow_candidates_path,
    )
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_sec_pit_evaluation_report(
            payload=payload,
            contributions=contributions,
            feature_contributions_path=feature_contributions_path,
            shadow_candidates_path=shadow_candidates_path,
        ),
        encoding="utf-8",
    )
    _write_run_log(
        run_log_path,
        status=str(payload["status"]),
        json_path=json_path,
        markdown_path=markdown_path,
        feature_contributions_path=feature_contributions_path,
        shadow_candidates_path=shadow_candidates_path,
        data_quality_report_path=quality_output,
    )
    return SecPitEvaluationArtifacts(
        status=str(payload["status"]),
        json_path=json_path,
        markdown_path=markdown_path,
        feature_contributions_path=feature_contributions_path,
        shadow_candidates_path=shadow_candidates_path,
        data_quality_report_path=quality_output,
        run_log_path=run_log_path,
    )


def render_sec_pit_evaluation_report(
    *,
    payload: dict[str, Any],
    contributions: pd.DataFrame | None = None,
    feature_contributions_path: Path | None = None,
    shadow_candidates_path: Path | None = None,
) -> str:
    metadata = payload["metadata"]
    policy = payload["policy"]
    data_quality = payload["data_quality"]
    pit_safety = payload.get("pit_safety", {})
    summary = payload.get("summary", {})
    lines = [
        "# SEC PIT 认知评估报告",
        "",
        f"- 状态：{payload['status']}",
        f"- 任务：`{SEC_PIT_EVALUATION_TASK_ID}`",
        f"- production_effect：`{SEC_PIT_EVALUATION_PRODUCTION_EFFECT}`",
        f"- backtest_data_grade：`{SEC_PIT_BACKTEST_DATA_GRADE}`",
        f"- 评估区间：{metadata['start']} 至 {metadata['end']}",
        f"- Benchmark：`{metadata['benchmark']}`",
        f"- Market regime：{metadata['market_regime_name']}（`{metadata['market_regime_id']}`）",
        f"- Regime 起点：{metadata['market_regime_start_date']}",
        f"- Policy：`{policy['policy_version']}`（{policy['status']}）",
        "",
        "## 数据质量门禁",
        "",
        f"- 状态：{data_quality['status']}",
        f"- 报告：`{data_quality['report_path']}`",
        f"- 错误数：{data_quality['error_count']}",
        f"- 警告数：{data_quality['warning_count']}",
        "",
        "## PIT 安全性",
        "",
        (
            "- future availability violation："
            f"{pit_safety.get('future_available_time_violation_count', 0)}"
        ),
        f"- invalid decision_date：{pit_safety.get('invalid_decision_date_count', 0)}",
        f"- missing available_time：{pit_safety.get('missing_available_time_count', 0)}",
        "- strict vendor archive：false",
        "",
    ]
    if payload["status"] in {"DATA_QUALITY_FAILED", "SAFETY_BLOCKED"}:
        lines.extend(
            [
                "## 阻断原因",
                "",
                str(payload.get("blocking_reason", "unknown")),
                "",
                "本次没有生成成功评价结论，也没有修改 production 权重或 scoring rules。",
            ]
        )
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "## 总结",
            "",
            f"- 结论：{summary['strategy_judgment_status']}",
            f"- 活跃 SEC ticker 数：{summary['active_ticker_count']}",
            f"- 可评估 signal_date 数：{summary['signal_date_count']}",
            f"- matched feature-return rows：{summary['matched_feature_return_rows']}",
            f"- shadow candidate：{summary['shadow_candidate_count']}",
            f"- research-only：{summary['research_only_count']}",
            f"- excluded：{summary['excluded_count']}",
            "",
            "## Feature 贡献",
            "",
        ]
    )
    if feature_contributions_path is not None:
        lines.append(f"- 明细 CSV：`{feature_contributions_path}`")
    if shadow_candidates_path is not None:
        lines.append(f"- Shadow candidate CSV：`{shadow_candidates_path}`")
    lines.append("")
    table_frame = (
        contributions
        if contributions is not None
        else pd.DataFrame(payload.get("feature_contributions", []))
    )
    if table_frame.empty:
        lines.append("没有可评价的 SEC PIT feature。")
    else:
        lines.extend(
            [
                (
                    "| Feature | Period | 分类 | obs | matched | mean rank IC | "
                    "IC 稳定性 | top-bottom excess | 原因 |"
                ),
                "|---|---|---|---:|---:|---:|---:|---:|---|",
            ]
        )
        for row in table_frame.head(20).to_dict(orient="records"):
            lines.append(
                "| "
                f"`{_escape_markdown_table(str(row['feature_id']))}` | "
                f"{_escape_markdown_table(str(row['period_type']))} | "
                f"`{row['classification']}` | "
                f"{int(row['observation_count'])} | "
                f"{int(row['matched_return_observation_count'])} | "
                f"{_format_optional_float(row['mean_daily_rank_ic'])} | "
                f"{_format_optional_float(row['rank_ic_sign_stability'])} | "
                f"{_format_optional_float(row['top_minus_bottom_forward_excess_return'])} | "
                f"{_escape_markdown_table(str(row['classification_reason']))} |"
            )

    lines.extend(
        [
            "",
            "## 解释边界",
            "",
            (
                "- 本报告只用于 research 和 shadow iteration 候选筛选，"
                "不构成 production 权重晋级证据。"
            ),
            (
                "- SEC EDGAR reconstructed PIT 数据是 B 级 filing-time PIT，"
                "不是 strict vendor archive。"
            ),
            (
                "- rank IC 和 top-bottom spread 使用未来收益做离线评价；"
                "不得被每日评分路径读取为当日可见信号。"
            ),
            "- shadow candidate 进入下一轮前仍需要独立 shadow outcome、稳健性和 owner review。",
        ]
    )
    if metadata["start"] < metadata["market_regime_start_date"]:
        lines.append(
            "- 请求起点早于默认 AI regime 起点；pre-regime 样本只能作为压力或对比样本解释。"
        )
    return "\n".join(lines) + "\n"


def _validate_policy(policy: SecPitEvaluationPolicy) -> None:
    if policy.forward_return_trading_days <= 0:
        raise ValueError("forward_return_trading_days must be positive")
    if policy.min_cross_sectional_tickers <= 1:
        raise ValueError("min_cross_sectional_tickers must be greater than 1")
    if policy.research_only_min_observations <= 0:
        raise ValueError("research_only_min_observations must be positive")
    if policy.shadow_candidate_min_observations < policy.research_only_min_observations:
        raise ValueError(
            "shadow_candidate_min_observations must be >= research_only_min_observations"
        )
    if policy.shadow_candidate_min_months <= 0:
        raise ValueError("shadow_candidate_min_months must be positive")
    for name, value in (
        ("shadow_candidate_min_ticker_coverage", policy.shadow_candidate_min_ticker_coverage),
        (
            "shadow_candidate_min_decision_day_coverage",
            policy.shadow_candidate_min_decision_day_coverage,
        ),
        (
            "shadow_candidate_min_rank_ic_sign_stability",
            policy.shadow_candidate_min_rank_ic_sign_stability,
        ),
        ("top_bottom_quantile", policy.top_bottom_quantile),
    ):
        if not 0 < value <= 1:
            raise ValueError(f"{name} must be in (0, 1]")


def _load_feature_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"SEC PIT feature panel does not exist: {path}")
    frame = pd.read_csv(path, dtype=str).fillna("")
    missing = sorted(set(SEC_PIT_FEATURE_PANEL_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"SEC PIT feature panel missing columns: {', '.join(missing)}")
    return frame


def _pit_safety_summary(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "row_count": 0,
            "invalid_decision_date_count": 0,
            "missing_available_time_count": 0,
            "future_available_time_violation_count": 0,
            "future_available_time_samples": [],
        }
    decision_dates = pd.to_datetime(frame["decision_date"], errors="coerce")
    available_times = pd.to_datetime(
        frame["max_input_available_time_utc"],
        errors="coerce",
        utc=True,
    )
    invalid_decision = decision_dates.isna()
    missing_available = available_times.isna()
    decision_date_values = decision_dates.dt.date
    available_date_values = available_times.dt.date
    future_mask = (
        decision_date_values.notna()
        & available_date_values.notna()
        & (available_date_values > decision_date_values)
    )
    samples = (
        frame.loc[
            future_mask,
            [
                "decision_date",
                "ticker",
                "feature_id",
                "period_type",
                "max_input_available_time_utc",
            ],
        ]
        .head(10)
        .to_dict(orient="records")
    )
    return {
        "row_count": len(frame),
        "invalid_decision_date_count": int(invalid_decision.sum()),
        "missing_available_time_count": int(missing_available.sum()),
        "future_available_time_violation_count": int(future_mask.sum()),
        "future_available_time_samples": samples,
    }


def _build_forward_excess_returns(
    *,
    prices: pd.DataFrame,
    tickers: list[str],
    benchmark: str,
    start: date,
    end: date,
    horizon_days: int,
) -> pd.DataFrame:
    required_tickers = set(tickers) | {benchmark}
    frame = prices.loc[prices["ticker"].astype(str).str.upper().isin(required_tickers)].copy()
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].sort_values(
        ["ticker", "_date"]
    )
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "decision_date",
                "return_date",
                "ticker",
                "forward_return",
                "benchmark_forward_return",
                "forward_excess_return",
            ]
        )
    frame["_future_date"] = frame.groupby("ticker")["_date"].shift(-horizon_days)
    frame["_future_adj_close"] = frame.groupby("ticker")["_adj_close"].shift(-horizon_days)
    frame["forward_return"] = frame["_future_adj_close"] / frame["_adj_close"] - 1.0
    forward = frame.loc[
        (frame["_date"].dt.date >= start)
        & (frame["_date"].dt.date <= end)
        & frame["forward_return"].notna()
    ].copy()
    benchmark_returns = forward.loc[
        forward["ticker"] == benchmark,
        ["_date", "forward_return"],
    ].rename(columns={"forward_return": "benchmark_forward_return"})
    merged = forward.loc[forward["ticker"].isin(tickers)].merge(
        benchmark_returns,
        on="_date",
        how="left",
    )
    merged["forward_excess_return"] = merged["forward_return"] - merged["benchmark_forward_return"]
    return pd.DataFrame(
        {
            "decision_date": merged["_date"].dt.date.astype(str),
            "return_date": merged["_future_date"].dt.date.astype(str),
            "ticker": merged["ticker"],
            "forward_return": merged["forward_return"],
            "benchmark_forward_return": merged["benchmark_forward_return"],
            "forward_excess_return": merged["forward_excess_return"],
        }
    )


def _prepare_feature_return_frame(
    *,
    feature_panel: pd.DataFrame,
    returns: pd.DataFrame,
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    requested = set(tickers)
    frame = feature_panel.copy()
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    frame["_decision_date"] = pd.to_datetime(frame["decision_date"], errors="coerce")
    frame["_feature_value"] = pd.to_numeric(frame["feature_value"], errors="coerce")
    frame = frame.loc[
        frame["_decision_date"].notna()
        & (frame["_decision_date"].dt.date >= start)
        & (frame["_decision_date"].dt.date <= end)
        & frame["ticker"].isin(requested)
        & frame["_feature_value"].notna()
    ].copy()
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "decision_date",
                "ticker",
                "feature_id",
                "period_type",
                "feature_value",
                "forward_excess_return",
            ]
        )
    frame["decision_date"] = frame["_decision_date"].dt.date.astype(str)
    merged = frame.merge(returns, on=["decision_date", "ticker"], how="left")
    merged["feature_value"] = merged["_feature_value"]
    return merged


def _feature_contributions(
    *,
    prepared: pd.DataFrame,
    tickers: list[str],
    signal_date_count: int,
    policy: SecPitEvaluationPolicy,
) -> pd.DataFrame:
    if prepared.empty:
        return pd.DataFrame(columns=list(SEC_PIT_FEATURE_CONTRIBUTION_COLUMNS))
    records: list[dict[str, Any]] = []
    expected_ticker_count = len(tickers)
    for (feature_id, period_type), group in prepared.groupby(
        ["feature_id", "period_type"],
        sort=True,
    ):
        evaluation_rows = group.loc[group["forward_excess_return"].notna()].copy()
        rank_ic_values = _daily_rank_ic_values(evaluation_rows, policy.min_cross_sectional_tickers)
        top_bottom_values = _daily_top_bottom_spreads(
            evaluation_rows,
            min_cross_sectional_tickers=policy.min_cross_sectional_tickers,
            top_bottom_quantile=policy.top_bottom_quantile,
        )
        mean_rank_ic = _mean_or_none(rank_ic_values)
        top_bottom_spread = _mean_or_none(top_bottom_values)
        monthly_rank_ic = _monthly_rank_ic(rank_ic_values)
        sign_stability = _rank_ic_sign_stability(mean_rank_ic, monthly_rank_ic)
        direction_adjusted_spread = _direction_adjusted_spread(mean_rank_ic, top_bottom_spread)
        unique_ticker_count = int(group["ticker"].nunique())
        unique_decision_day_count = int(group["decision_date"].nunique())
        ticker_coverage = (
            unique_ticker_count / expected_ticker_count if expected_ticker_count else 0.0
        )
        decision_day_coverage = (
            unique_decision_day_count / signal_date_count if signal_date_count else 0.0
        )
        feature_value_unique_count = int(evaluation_rows["feature_value"].nunique())
        classification, reason, action = _classify_feature(
            matched_return_observation_count=len(evaluation_rows),
            feature_value_unique_count=feature_value_unique_count,
            ticker_coverage=ticker_coverage,
            decision_day_coverage=decision_day_coverage,
            mean_rank_ic=mean_rank_ic,
            monthly_rank_ic_count=len(monthly_rank_ic),
            rank_ic_sign_stability=sign_stability,
            direction_adjusted_spread=direction_adjusted_spread,
            policy=policy,
        )
        records.append(
            {
                "feature_id": str(feature_id),
                "period_type": str(period_type),
                "classification": classification,
                "classification_reason": reason,
                "recommended_action": action,
                "observation_count": len(group),
                "matched_return_observation_count": len(evaluation_rows),
                "unique_ticker_count": unique_ticker_count,
                "unique_decision_day_count": unique_decision_day_count,
                "ticker_coverage_rate": ticker_coverage,
                "decision_day_coverage_rate": decision_day_coverage,
                "mean_daily_rank_ic": mean_rank_ic,
                "rank_ic_day_count": len(rank_ic_values),
                "monthly_rank_ic_count": len(monthly_rank_ic),
                "rank_ic_sign_stability": sign_stability,
                "top_minus_bottom_forward_excess_return": top_bottom_spread,
                "direction_adjusted_top_bottom_forward_excess_return": (direction_adjusted_spread),
                "feature_value_unique_count": feature_value_unique_count,
                "pit_safety_issue_count": 0,
            }
        )
    frame = pd.DataFrame(records, columns=list(SEC_PIT_FEATURE_CONTRIBUTION_COLUMNS))
    if frame.empty:
        return frame
    rank = frame["direction_adjusted_top_bottom_forward_excess_return"].fillna(-999.0)
    classification_order = {
        "SHADOW_CANDIDATE": 0,
        "RESEARCH_ONLY": 1,
        "EXCLUDED": 2,
    }
    return (
        frame.assign(_rank=rank)
        .assign(_classification_rank=frame["classification"].map(classification_order).fillna(9))
        .sort_values(
            ["_classification_rank", "_rank", "feature_id", "period_type"],
            ascending=[True, False, True, True],
        )
        .drop(columns=["_rank", "_classification_rank"])
        .reset_index(drop=True)
    )


def _daily_rank_ic_values(
    frame: pd.DataFrame,
    min_cross_sectional_tickers: int,
) -> dict[str, float]:
    values: dict[str, float] = {}
    for signal_date, group in frame.groupby("decision_date"):
        clean = group.loc[
            group["feature_value"].notna() & group["forward_excess_return"].notna()
        ].copy()
        if len(clean) < min_cross_sectional_tickers:
            continue
        if clean["feature_value"].nunique() < 2 or clean["forward_excess_return"].nunique() < 2:
            continue
        feature_rank = clean["feature_value"].rank(method="average")
        return_rank = clean["forward_excess_return"].rank(method="average")
        rank_ic = feature_rank.corr(return_rank)
        if pd.notna(rank_ic):
            values[str(signal_date)] = float(rank_ic)
    return values


def _daily_top_bottom_spreads(
    frame: pd.DataFrame,
    *,
    min_cross_sectional_tickers: int,
    top_bottom_quantile: float,
) -> dict[str, float]:
    values: dict[str, float] = {}
    for signal_date, group in frame.groupby("decision_date"):
        clean = group.loc[
            group["feature_value"].notna() & group["forward_excess_return"].notna()
        ].copy()
        if len(clean) < min_cross_sectional_tickers:
            continue
        clean = clean.sort_values("feature_value")
        bucket_size = max(1, int(len(clean) * top_bottom_quantile))
        bottom = clean.head(bucket_size)
        top = clean.tail(bucket_size)
        spread = top["forward_excess_return"].mean() - bottom["forward_excess_return"].mean()
        if pd.notna(spread):
            values[str(signal_date)] = float(spread)
    return values


def _monthly_rank_ic(rank_ic_values: dict[str, float]) -> dict[str, float]:
    by_month: dict[str, list[float]] = {}
    for signal_date, value in rank_ic_values.items():
        month = signal_date[:7]
        by_month.setdefault(month, []).append(value)
    return {month: sum(values) / len(values) for month, values in by_month.items()}


def _rank_ic_sign_stability(
    mean_rank_ic: float | None,
    monthly_rank_ic: dict[str, float],
) -> float | None:
    if mean_rank_ic is None or not monthly_rank_ic:
        return None
    if mean_rank_ic == 0:
        return 0.0
    expected_positive = mean_rank_ic > 0
    same_sign_count = sum(
        1
        for value in monthly_rank_ic.values()
        if (value > 0 and expected_positive) or (value < 0 and not expected_positive)
    )
    return same_sign_count / len(monthly_rank_ic)


def _classify_feature(
    *,
    matched_return_observation_count: int,
    feature_value_unique_count: int,
    ticker_coverage: float,
    decision_day_coverage: float,
    mean_rank_ic: float | None,
    monthly_rank_ic_count: int,
    rank_ic_sign_stability: float | None,
    direction_adjusted_spread: float | None,
    policy: SecPitEvaluationPolicy,
) -> tuple[str, str, str]:
    if matched_return_observation_count < policy.research_only_min_observations:
        return (
            "EXCLUDED",
            "matched return observations below research-only sample floor",
            "排除出 shadow iteration；补齐样本后重新评估。",
        )
    if feature_value_unique_count < 2:
        return (
            "EXCLUDED",
            "feature value has insufficient variation",
            "排除出 shadow iteration；先复核 feature 定义或数据覆盖。",
        )
    if mean_rank_ic is None or rank_ic_sign_stability is None or direction_adjusted_spread is None:
        return (
            "RESEARCH_ONLY",
            "rank IC or top-bottom spread could not be estimated robustly",
            "保留 research-only；等待更多横截面样本。",
        )
    if matched_return_observation_count < policy.shadow_candidate_min_observations:
        return (
            "RESEARCH_ONLY",
            "sample count below shadow candidate floor",
            "保留 research-only；继续积累 SEC PIT panel 和 forward return 样本。",
        )
    if ticker_coverage < policy.shadow_candidate_min_ticker_coverage:
        return (
            "RESEARCH_ONLY",
            "ticker coverage below shadow candidate floor",
            "保留 research-only；优先改善 universe 覆盖。",
        )
    if decision_day_coverage < policy.shadow_candidate_min_decision_day_coverage:
        return (
            "RESEARCH_ONLY",
            "decision-day coverage below shadow candidate floor",
            "保留 research-only；优先改善 PIT panel 时间覆盖。",
        )
    if monthly_rank_ic_count < policy.shadow_candidate_min_months:
        return (
            "RESEARCH_ONLY",
            "monthly IC history below shadow candidate floor",
            "保留 research-only；等待更多月份样本。",
        )
    if abs(mean_rank_ic) < policy.shadow_candidate_min_abs_mean_rank_ic:
        return (
            "RESEARCH_ONLY",
            "absolute mean rank IC below shadow threshold",
            "保留 research-only；不进入 shadow weight iteration。",
        )
    if rank_ic_sign_stability < policy.shadow_candidate_min_rank_ic_sign_stability:
        return (
            "RESEARCH_ONLY",
            "rank IC sign stability below shadow threshold",
            "保留 research-only；先做 regime/month 分层复核。",
        )
    if direction_adjusted_spread <= policy.shadow_candidate_min_directional_spread:
        return (
            "RESEARCH_ONLY",
            "direction-adjusted top-bottom spread is not positive enough",
            "保留 research-only；先检查方向性和经济含义。",
        )
    return (
        "SHADOW_CANDIDATE",
        "coverage, rank IC, stability, and directional spread meet shadow policy",
        "纳入 shadow weight iteration 候选；不得直接进入 production 权重。",
    )


def _evaluation_payload(
    *,
    start: date,
    end: date,
    feature_panel_path: Path,
    universe_path: Path,
    benchmark: str,
    policy: SecPitEvaluationPolicy,
    selected_regime: Any,
    data_quality_report: DataQualityReport,
    data_quality_report_path: Path,
    pit_safety: dict[str, Any],
    contributions: pd.DataFrame,
    signal_date_count: int,
    active_tickers: list[str],
    matched_feature_return_rows: int,
    feature_contributions_path: Path,
    shadow_candidates_path: Path,
) -> dict[str, Any]:
    classification_counts = Counter(contributions["classification"].astype(str))
    shadow_count = classification_counts.get("SHADOW_CANDIDATE", 0)
    strategy_judgment_status = (
        "POSITIVE_SHADOW_EVIDENCE"
        if shadow_count >= policy.min_shadow_candidate_count_for_positive_loop
        else "MIXED_OR_INSUFFICIENT_EVIDENCE"
    )
    status = "PASS" if shadow_count else "PASS_WITH_LIMITATIONS"
    return {
        "status": status,
        "task_id": SEC_PIT_EVALUATION_TASK_ID,
        "production_effect": SEC_PIT_EVALUATION_PRODUCTION_EFFECT,
        "metadata": _metadata_payload(
            start=start,
            end=end,
            feature_panel_path=feature_panel_path,
            universe_path=universe_path,
            benchmark=benchmark,
            selected_regime=selected_regime,
        ),
        "policy": _policy_payload(policy),
        "data_quality": _data_quality_payload(data_quality_report, data_quality_report_path),
        "pit_safety": pit_safety,
        "summary": {
            "strategy_judgment_status": strategy_judgment_status,
            "active_ticker_count": len(active_tickers),
            "signal_date_count": signal_date_count,
            "matched_feature_return_rows": matched_feature_return_rows,
            "shadow_candidate_count": shadow_count,
            "research_only_count": classification_counts.get("RESEARCH_ONLY", 0),
            "excluded_count": classification_counts.get("EXCLUDED", 0),
            "feature_contributions_path": str(feature_contributions_path),
            "shadow_candidates_path": str(shadow_candidates_path),
        },
        "feature_contributions": _records_for_json(contributions),
    }


def _blocked_payload(
    *,
    status: str,
    start: date,
    end: date,
    feature_panel_path: Path,
    universe_path: Path,
    benchmark: str,
    policy: SecPitEvaluationPolicy,
    selected_regime: Any,
    data_quality_report: DataQualityReport,
    data_quality_report_path: Path,
    blocking_reason: str,
    pit_safety: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "task_id": SEC_PIT_EVALUATION_TASK_ID,
        "production_effect": SEC_PIT_EVALUATION_PRODUCTION_EFFECT,
        "metadata": _metadata_payload(
            start=start,
            end=end,
            feature_panel_path=feature_panel_path,
            universe_path=universe_path,
            benchmark=benchmark,
            selected_regime=selected_regime,
        ),
        "policy": _policy_payload(policy),
        "data_quality": _data_quality_payload(data_quality_report, data_quality_report_path),
        "pit_safety": pit_safety
        or {
            "row_count": 0,
            "invalid_decision_date_count": 0,
            "missing_available_time_count": 0,
            "future_available_time_violation_count": 0,
            "future_available_time_samples": [],
        },
        "blocking_reason": blocking_reason,
        "summary": {
            "strategy_judgment_status": status,
            "shadow_candidate_count": 0,
            "research_only_count": 0,
            "excluded_count": 0,
        },
        "feature_contributions": [],
    }


def _write_blocked_outputs(
    *,
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
    run_log_path: Path,
) -> None:
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_sec_pit_evaluation_report(payload=payload),
        encoding="utf-8",
    )
    _write_run_log(
        run_log_path,
        status=str(payload["status"]),
        json_path=json_path,
        markdown_path=markdown_path,
        feature_contributions_path=None,
        shadow_candidates_path=None,
        data_quality_report_path=Path(payload["data_quality"]["report_path"]),
    )


def _metadata_payload(
    *,
    start: date,
    end: date,
    feature_panel_path: Path,
    universe_path: Path,
    benchmark: str,
    selected_regime: Any,
) -> dict[str, Any]:
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
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
        "generated_at": datetime.now(UTC).isoformat(),
    }


def _policy_payload(policy: SecPitEvaluationPolicy) -> dict[str, Any]:
    return {
        "policy_version": policy.policy_version,
        "owner": policy.owner,
        "status": policy.status,
        "rationale": policy.rationale,
        "review_condition": policy.review_condition,
        "forward_return_trading_days": policy.forward_return_trading_days,
        "min_cross_sectional_tickers": policy.min_cross_sectional_tickers,
        "research_only_min_observations": policy.research_only_min_observations,
        "shadow_candidate_min_observations": policy.shadow_candidate_min_observations,
        "shadow_candidate_min_months": policy.shadow_candidate_min_months,
        "shadow_candidate_min_ticker_coverage": policy.shadow_candidate_min_ticker_coverage,
        "shadow_candidate_min_decision_day_coverage": (
            policy.shadow_candidate_min_decision_day_coverage
        ),
        "shadow_candidate_min_abs_mean_rank_ic": policy.shadow_candidate_min_abs_mean_rank_ic,
        "shadow_candidate_min_rank_ic_sign_stability": (
            policy.shadow_candidate_min_rank_ic_sign_stability
        ),
        "shadow_candidate_min_directional_spread": (policy.shadow_candidate_min_directional_spread),
        "top_bottom_quantile": policy.top_bottom_quantile,
        "min_shadow_candidate_count_for_positive_loop": (
            policy.min_shadow_candidate_count_for_positive_loop
        ),
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


def _records_for_json(frame: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        records.append({key: (None if pd.isna(value) else value) for key, value in record.items()})
    return records


def _mean_or_none(values: dict[str, float]) -> float | None:
    if not values:
        return None
    return sum(values.values()) / len(values)


def _direction_adjusted_spread(
    mean_rank_ic: float | None,
    top_bottom_spread: float | None,
) -> float | None:
    if mean_rank_ic is None or top_bottom_spread is None:
        return None
    if mean_rank_ic < 0:
        return -top_bottom_spread
    return top_bottom_spread


def _write_run_log(
    path: Path,
    *,
    status: str,
    json_path: Path,
    markdown_path: Path,
    feature_contributions_path: Path | None,
    shadow_candidates_path: Path | None,
    data_quality_report_path: Path,
) -> None:
    lines = [
        f"generated_at={datetime.now(UTC).isoformat()}",
        f"task_id={SEC_PIT_EVALUATION_TASK_ID}",
        f"status={status}",
        f"json={json_path}",
        f"markdown={markdown_path}",
        f"feature_contributions={feature_contributions_path or ''}",
        f"shadow_candidates={shadow_candidates_path or ''}",
        f"data_quality_report={data_quality_report_path}",
        f"production_effect={SEC_PIT_EVALUATION_PRODUCTION_EFFECT}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _format_optional_float(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        if pd.isna(value):
            return "n/a"
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return "n/a"


def _escape_markdown_table(value: str) -> str:
    return value.replace("\n", " ").replace("|", "\\|")
