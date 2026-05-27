from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.config import (
    FeatureConfig,
    PortfolioConfig,
    ScoringRulesConfig,
)
from ai_trading_system.data.quality import (
    DataFileSummary,
    DataQualityIssue,
    DataQualityReport,
    Severity,
)
from ai_trading_system.features.market import build_market_features
from ai_trading_system.fundamentals.sec_pit_aliases import (
    canonicalize_ticker_series,
    load_ticker_aliases,
)
from ai_trading_system.fundamentals.sec_pit_baseline_comparison import (
    ACTION_POSITIVE_SCORE_MIN,
    ACTION_WATCH_SCORE_MIN,
)
from ai_trading_system.scoring.daily import DailyScoreReport, build_daily_score_report

BASELINE_SCORE_BACKFILL_TASK_ID = "TRADING-045"
BASELINE_SCORE_BACKFILL_MODE = "research_backfill"
BASELINE_SCORE_BACKFILL_SOURCE = "score_daily_market_baseline_research_backfill"
BASELINE_SCORE_BACKFILL_PRODUCTION_EFFECT = "none"

BASELINE_SCORE_BACKFILL_COLUMNS: tuple[str, ...] = (
    "decision_date",
    "ticker",
    "baseline_score",
    "baseline_rank",
    "baseline_action",
    "score_source",
    "score_version",
    "input_feature_count",
    "available_feature_count",
    "missing_feature_count",
    "score_completeness_ratio",
    "research_backfill",
    "production_effect",
    "generated_at",
    "data_quality_status",
    "data_quality_report_path",
    "baseline_limitations",
    "market_score",
    "technical_score",
    "risk_score",
    "valuation_score",
    "momentum_score",
    "fundamental_score",
    "news_score",
    "macro_score",
)


@dataclass(frozen=True)
class BaselineScoreBackfillResult:
    output_path: Path
    row_count: int
    ticker_count: int
    date_count: int
    start_date: date
    end_date: date
    production_effect: str = BASELINE_SCORE_BACKFILL_PRODUCTION_EFFECT
    research_backfill: bool = True


def run_baseline_score_backfill(
    *,
    start: date,
    end: date,
    tickers: list[str],
    prices_path: Path,
    rates_path: Path,
    output_path: Path,
    feature_config: FeatureConfig,
    scoring_rules: ScoringRulesConfig,
    portfolio: PortfolioConfig,
    data_quality_status: str,
    data_quality_report_path: Path | None = None,
    mode: str = BASELINE_SCORE_BACKFILL_MODE,
    overwrite: bool = False,
) -> BaselineScoreBackfillResult:
    if start > end:
        raise ValueError("start must be on or before end")
    if mode != BASELINE_SCORE_BACKFILL_MODE:
        raise ValueError("baseline score backfill only supports mode=research_backfill")
    if not tickers:
        raise ValueError("at least one ticker is required for baseline score backfill")
    if output_path.exists() and not overwrite:
        raise FileExistsError(
            f"{output_path} already exists; pass --overwrite to replace it with research backfill"
        )

    prices = pd.read_csv(prices_path)
    rates = pd.read_csv(rates_path)
    normalized_tickers = _canonical_tickers(tickers)
    decision_dates = _decision_dates(prices, normalized_tickers, start, end)
    rows: list[dict[str, Any]] = []
    for decision_date in decision_dates:
        report = _build_daily_report_for_backfill(
            prices=prices,
            rates=rates,
            feature_config=feature_config,
            scoring_rules=scoring_rules,
            portfolio=portfolio,
            tickers=normalized_tickers,
            as_of=decision_date,
            data_quality_status=data_quality_status,
        )
        rows.extend(
            _date_records(
                report=report,
                tickers=normalized_tickers,
                prices=prices,
                decision_date=decision_date,
                end=end,
                score_version=_score_version(scoring_rules),
                data_quality_status=data_quality_status,
                data_quality_report_path=data_quality_report_path,
            )
        )

    frame = pd.DataFrame(rows, columns=list(BASELINE_SCORE_BACKFILL_COLUMNS))
    if not frame.empty:
        frame = _assign_ranks(frame)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return BaselineScoreBackfillResult(
        output_path=output_path,
        row_count=int(len(frame)),
        ticker_count=len(normalized_tickers),
        date_count=len(decision_dates),
        start_date=start,
        end_date=end,
    )


def _build_daily_report_for_backfill(
    *,
    prices: pd.DataFrame,
    rates: pd.DataFrame,
    feature_config: FeatureConfig,
    scoring_rules: ScoringRulesConfig,
    portfolio: PortfolioConfig,
    tickers: list[str],
    as_of: date,
    data_quality_status: str,
) -> DailyScoreReport:
    feature_set = build_market_features(
        prices=prices,
        rates=rates,
        config=feature_config,
        as_of=as_of,
        core_watchlist=tickers,
    )
    return build_daily_score_report(
        feature_set=feature_set,
        data_quality_report=_backfill_data_quality_report(
            prices=prices,
            rates=rates,
            as_of=as_of,
            tickers=tickers,
            status=data_quality_status,
        ),
        rules=scoring_rules,
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
        max_total_ai_exposure=portfolio.position_limits.max_total_ai_exposure,
        macro_risk_asset_budget=portfolio.macro_risk_asset_budget,
        risk_budget=portfolio.risk_budget,
    )


def _date_records(
    *,
    report: DailyScoreReport,
    tickers: list[str],
    prices: pd.DataFrame,
    decision_date: date,
    end: date,
    score_version: str,
    data_quality_status: str,
    data_quality_report_path: Path | None,
) -> list[dict[str, Any]]:
    component_scores = {component.name: component.score for component in report.components}
    signal_count = sum(len(component.signals) for component in report.components)
    available_signal_count = sum(
        1 for component in report.components for signal in component.signals if signal.available
    )
    ticker_price_available = _ticker_price_availability(prices, tickers, decision_date)
    generated_at = _deterministic_generated_at(end)
    records: list[dict[str, Any]] = []
    for ticker in tickers:
        price_available = ticker_price_available.get(ticker, False)
        input_feature_count = signal_count + 1
        available_feature_count = available_signal_count + (1 if price_available else 0)
        missing_feature_count = input_feature_count - available_feature_count
        completeness = available_feature_count / input_feature_count if input_feature_count else 0.0
        limitations = _limitations(report, price_available)
        records.append(
            {
                "decision_date": decision_date.isoformat(),
                "ticker": ticker,
                "baseline_score": round(float(report.recommendation.total_score), 6),
                "baseline_rank": np.nan,
                "baseline_action": _baseline_action(report.recommendation.total_score),
                "score_source": BASELINE_SCORE_BACKFILL_SOURCE,
                "score_version": score_version,
                "input_feature_count": input_feature_count,
                "available_feature_count": available_feature_count,
                "missing_feature_count": missing_feature_count,
                "score_completeness_ratio": round(float(completeness), 6),
                "research_backfill": True,
                "production_effect": BASELINE_SCORE_BACKFILL_PRODUCTION_EFFECT,
                "generated_at": generated_at,
                "data_quality_status": data_quality_status,
                "data_quality_report_path": (
                    "" if data_quality_report_path is None else str(data_quality_report_path)
                ),
                "baseline_limitations": "；".join(limitations),
                "market_score": round(float(report.recommendation.total_score), 6),
                "technical_score": _optional_score(component_scores.get("trend")),
                "risk_score": _optional_score(component_scores.get("risk_sentiment")),
                "valuation_score": _optional_score(component_scores.get("valuation")),
                "momentum_score": _optional_score(component_scores.get("trend")),
                "fundamental_score": _optional_score(component_scores.get("fundamentals")),
                "news_score": "",
                "macro_score": _optional_score(component_scores.get("macro_liquidity")),
            }
        )
    return records


def _assign_ranks(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["_score"] = pd.to_numeric(result["baseline_score"], errors="coerce")
    result = result.sort_values(
        ["decision_date", "_score", "ticker"],
        ascending=[True, False, True],
    )
    result["baseline_rank"] = result.groupby("decision_date").cumcount() + 1
    return result.drop(columns=["_score"]).loc[:, list(BASELINE_SCORE_BACKFILL_COLUMNS)]


def _decision_dates(
    prices: pd.DataFrame,
    tickers: list[str],
    start: date,
    end: date,
) -> list[date]:
    required = {"date", "ticker", "adj_close"}
    missing = sorted(required - set(prices.columns))
    if missing:
        raise ValueError(f"prices CSV is missing required columns: {', '.join(missing)}")
    frame = prices.copy()
    aliases = load_ticker_aliases()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["_ticker"] = canonicalize_ticker_series(frame["ticker"], aliases=aliases)
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[
        frame["_date"].notna()
        & frame["_ticker"].isin(tickers)
        & frame["_adj_close"].notna()
        & frame["_date"].map(lambda value: start <= value <= end)
    ].copy()
    return sorted(frame["_date"].dropna().unique().tolist())


def _ticker_price_availability(
    prices: pd.DataFrame,
    tickers: list[str],
    decision_date: date,
) -> dict[str, bool]:
    aliases = load_ticker_aliases()
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["_ticker"] = canonicalize_ticker_series(frame["ticker"], aliases=aliases)
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    available = set(
        frame.loc[
            (frame["_date"] == decision_date)
            & frame["_ticker"].isin(tickers)
            & frame["_adj_close"].notna(),
            "_ticker",
        ]
        .dropna()
        .astype(str)
    )
    return {ticker: ticker in available for ticker in tickers}


def _canonical_tickers(tickers: list[str]) -> list[str]:
    aliases = load_ticker_aliases()
    series = pd.Series([str(ticker).upper() for ticker in tickers])
    canonical = canonicalize_ticker_series(series, aliases=aliases).astype(str).str.upper()
    return sorted(dict.fromkeys(canonical.tolist()))


def _backfill_data_quality_report(
    *,
    prices: pd.DataFrame,
    rates: pd.DataFrame,
    as_of: date,
    tickers: list[str],
    status: str,
) -> DataQualityReport:
    issues: tuple[DataQualityIssue, ...]
    if status == "PASS_WITH_WARNINGS":
        issues = (
            DataQualityIssue(
                severity=Severity.WARNING,
                code="research_backfill_source_warning",
                message="Backfill caller reported PASS_WITH_WARNINGS for source data.",
            ),
        )
    elif status == "FAIL":
        issues = (
            DataQualityIssue(
                severity=Severity.ERROR,
                code="research_backfill_source_failed",
                message="Backfill caller reported FAIL for source data.",
            ),
        )
    else:
        issues = ()
    return DataQualityReport(
        checked_at=datetime(as_of.year, as_of.month, as_of.day, tzinfo=UTC),
        as_of=as_of,
        price_summary=DataFileSummary(path=Path("prices_daily.csv"), exists=True, rows=len(prices)),
        rate_summary=DataFileSummary(path=Path("rates_daily.csv"), exists=True, rows=len(rates)),
        expected_price_tickers=tuple(tickers),
        expected_rate_series=tuple(
            sorted(rates.get("series", pd.Series(dtype=str)).dropna().astype(str).unique())
        ),
        issues=issues,
    )


def _limitations(report: DailyScoreReport, price_available: bool) -> list[str]:
    limitations = [
        "research_backfill_only",
        "market_wide_score_replicated_by_ticker",
        "production_effect_none",
    ]
    if not price_available:
        limitations.append("ticker_price_missing_on_decision_date")
    if any(component.source_type != "hard_data" for component in report.components):
        limitations.append("degraded_or_placeholder_components_present")
    return limitations


def _baseline_action(score: float) -> str:
    if score >= ACTION_POSITIVE_SCORE_MIN:
        return "REVIEW_POSITIVE"
    if score >= ACTION_WATCH_SCORE_MIN:
        return "WATCH"
    return "RESEARCH_ONLY"


def _score_version(scoring_rules: ScoringRulesConfig) -> str:
    metadata = scoring_rules.policy_metadata.model_dump(mode="json")
    return str(metadata.get("version") or "scoring_rules_unknown")


def _optional_score(value: object) -> object:
    if value is None or pd.isna(value):
        return ""
    return round(float(value), 6)


def _deterministic_generated_at(end: date) -> str:
    return datetime(end.year, end.month, end.day, tzinfo=UTC).isoformat()
