from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import pandas as pd

from ai_trading_system.config import FeatureConfig
from ai_trading_system.data.quality import DataQualityReport


@dataclass(frozen=True)
class MarketFeatureRow:
    as_of: date
    source_date: date
    category: str
    subject: str
    feature: str
    value: float
    unit: str
    lookback: int | None
    source: str
    notes: str = ""

    def to_record(self) -> dict[str, object]:
        return {
            "as_of": self.as_of.isoformat(),
            "source_date": self.source_date.isoformat(),
            "category": self.category,
            "subject": self.subject,
            "feature": self.feature,
            "value": self.value,
            "unit": self.unit,
            "lookback": self.lookback,
            "source": self.source,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class FeatureWarning:
    code: str
    message: str
    subject: str
    feature: str
    required_observations: int
    available_observations: int


@dataclass(frozen=True)
class MarketFeatureSet:
    as_of: date
    rows: tuple[MarketFeatureRow, ...]
    warnings: tuple[FeatureWarning, ...]

    @property
    def status(self) -> str:
        if self.warnings:
            return "PASS_WITH_WARNINGS"
        return "PASS"

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame([row.to_record() for row in self.rows])


def build_market_features(
    prices: pd.DataFrame,
    rates: pd.DataFrame,
    config: FeatureConfig,
    as_of: date,
    core_watchlist: list[str],
) -> MarketFeatureSet:
    price_frame = _prepare_prices(prices, as_of)
    rate_frame = _prepare_rates(rates, as_of)
    rows: list[MarketFeatureRow] = []
    warnings: list[FeatureWarning] = []

    _append_ticker_features(price_frame, config, as_of, rows, warnings)
    _append_relative_strength_features(price_frame, config, as_of, rows, warnings)
    _append_vix_features(price_frame, config, as_of, rows, warnings)
    _append_rate_features(rate_frame, config, as_of, rows, warnings)
    _append_core_breadth_features(price_frame, config, core_watchlist, as_of, rows, warnings)

    if not rows:
        raise ValueError("no market features were generated")

    return MarketFeatureSet(as_of=as_of, rows=tuple(rows), warnings=tuple(warnings))


def write_features_csv(feature_set: MarketFeatureSet, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = feature_set.to_frame()

    if output_path.exists():
        existing = pd.read_csv(output_path)
        if "as_of" not in existing.columns:
            raise ValueError(f"existing feature file is missing as_of column: {output_path}")
        existing = existing.loc[existing["as_of"] != feature_set.as_of.isoformat()]
        new_frame = pd.concat([existing, new_frame], ignore_index=True)

    new_frame = new_frame.sort_values(["as_of", "category", "subject", "feature"]).reset_index(
        drop=True
    )
    new_frame.to_csv(output_path, index=False)
    return output_path


def render_feature_summary(
    feature_set: MarketFeatureSet,
    data_quality_report: DataQualityReport,
    data_quality_report_path: Path,
    features_path: Path,
) -> str:
    rows_by_category = _count_by([row.category for row in feature_set.rows])
    lines = [
        "# Market Feature Summary",
        "",
        f"- Status: {feature_set.status}",
        f"- As of: {feature_set.as_of.isoformat()}",
        f"- Feature rows: {len(feature_set.rows)}",
        f"- Feature warnings: {len(feature_set.warnings)}",
        f"- Data quality status: {data_quality_report.status}",
        f"- Data quality report: `{data_quality_report_path}`",
        f"- Feature output: `{features_path}`",
        "",
        "## Feature Rows By Category",
        "",
    ]

    for category, count in rows_by_category.items():
        lines.append(f"- {category}: {count}")

    lines.extend(["", "## Warnings", ""])
    if not feature_set.warnings:
        lines.append("No feature warnings.")
    else:
        lines.extend(
            [
                "| Code | Subject | Feature | Required | Available | Message |",
                "|---|---|---|---:|---:|---|",
            ]
        )
        for warning in feature_set.warnings:
            lines.append(
                "| "
                f"{warning.code} | "
                f"{warning.subject} | "
                f"{warning.feature} | "
                f"{warning.required_observations} | "
                f"{warning.available_observations} | "
                f"{_escape_markdown_table(warning.message)} |"
            )

    return "\n".join(lines) + "\n"


def write_feature_summary(
    feature_set: MarketFeatureSet,
    data_quality_report: DataQualityReport,
    data_quality_report_path: Path,
    features_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_feature_summary(
            feature_set,
            data_quality_report=data_quality_report,
            data_quality_report_path=data_quality_report_path,
            features_path=features_path,
        ),
        encoding="utf-8",
    )
    return output_path


def default_feature_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"feature_summary_{as_of.isoformat()}.md"


def _prepare_prices(prices: pd.DataFrame, as_of: date) -> pd.DataFrame:
    required_columns = {"date", "ticker", "adj_close", "close"}
    missing = sorted(required_columns - set(prices.columns))
    if missing:
        raise ValueError(f"prices missing required columns: {', '.join(missing)}")

    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame["_close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.loc[
        frame["_date"].notna()
        & frame["_adj_close"].notna()
        & frame["_close"].notna()
        & (frame["_date"] <= pd.Timestamp(as_of))
    ].copy()
    return frame.sort_values(["ticker", "_date"]).reset_index(drop=True)


def _prepare_rates(rates: pd.DataFrame, as_of: date) -> pd.DataFrame:
    required_columns = {"date", "series", "value"}
    missing = sorted(required_columns - set(rates.columns))
    if missing:
        raise ValueError(f"rates missing required columns: {', '.join(missing)}")

    frame = rates.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame = frame.loc[
        frame["_date"].notna()
        & frame["_value"].notna()
        & (frame["_date"] <= pd.Timestamp(as_of))
    ].copy()
    return frame.sort_values(["series", "_date"]).reset_index(drop=True)


def _append_ticker_features(
    prices: pd.DataFrame,
    config: FeatureConfig,
    as_of: date,
    rows: list[MarketFeatureRow],
    warnings: list[FeatureWarning],
) -> None:
    for ticker, group in prices.groupby("ticker", sort=True):
        history = group.sort_values("_date").reset_index(drop=True)
        if history.empty:
            continue

        latest = history.iloc[-1]
        source_date = _as_date(latest["_date"])
        latest_adj_close = float(latest["_adj_close"])
        rows.append(
            MarketFeatureRow(
                as_of=as_of,
                source_date=source_date,
                category="price",
                subject=str(ticker),
                feature="adj_close",
                value=latest_adj_close,
                unit="price",
                lookback=None,
                source="prices_daily",
            )
        )

        for window in config.moving_average_windows:
            ma = _trailing_mean(
                history["_adj_close"],
                window,
                str(ticker),
                f"ma_{window}",
                warnings,
            )
            if ma is None:
                continue
            rows.extend(
                [
                    MarketFeatureRow(
                        as_of=as_of,
                        source_date=source_date,
                        category="trend",
                        subject=str(ticker),
                        feature=f"ma_{window}",
                        value=ma,
                        unit="price",
                        lookback=window,
                        source="prices_daily",
                    ),
                    MarketFeatureRow(
                        as_of=as_of,
                        source_date=source_date,
                        category="trend",
                        subject=str(ticker),
                        feature=f"above_ma_{window}",
                        value=1.0 if latest_adj_close > ma else 0.0,
                        unit="boolean",
                        lookback=window,
                        source="prices_daily",
                    ),
                    MarketFeatureRow(
                        as_of=as_of,
                        source_date=source_date,
                        category="trend",
                        subject=str(ticker),
                        feature=f"pct_vs_ma_{window}",
                        value=(latest_adj_close / ma) - 1.0,
                        unit="ratio",
                        lookback=window,
                        source="prices_daily",
                    ),
                ]
            )

        for window in config.return_windows:
            period_return = _trailing_return(
                history["_adj_close"],
                window,
                str(ticker),
                f"return_{window}d",
                warnings,
            )
            if period_return is None:
                continue
            rows.append(
                MarketFeatureRow(
                    as_of=as_of,
                    source_date=source_date,
                    category="trend",
                    subject=str(ticker),
                    feature=f"return_{window}d",
                    value=period_return,
                    unit="ratio",
                    lookback=window,
                    source="prices_daily",
                )
            )


def _append_relative_strength_features(
    prices: pd.DataFrame,
    config: FeatureConfig,
    as_of: date,
    rows: list[MarketFeatureRow],
    warnings: list[FeatureWarning],
) -> None:
    if prices.empty:
        return

    pivot = prices.pivot(index="_date", columns="ticker", values="_adj_close").sort_index()
    for pair in config.relative_strength_pairs:
        subject = f"{pair.numerator}/{pair.denominator}"
        if pair.numerator not in pivot.columns or pair.denominator not in pivot.columns:
            warnings.append(
                FeatureWarning(
                    code="relative_strength_missing_ticker",
                    message="relative strength pair is missing one or both tickers",
                    subject=subject,
                    feature="relative_strength_ratio",
                    required_observations=1,
                    available_observations=0,
                )
            )
            continue

        ratio = (pivot[pair.numerator] / pivot[pair.denominator]).dropna()
        if ratio.empty:
            warnings.append(
                FeatureWarning(
                    code="relative_strength_no_overlap",
                    message="relative strength pair has no overlapping dates",
                    subject=subject,
                    feature="relative_strength_ratio",
                    required_observations=1,
                    available_observations=0,
                )
            )
            continue

        source_date = _as_date(ratio.index[-1])
        rows.append(
            MarketFeatureRow(
                as_of=as_of,
                source_date=source_date,
                category="relative_strength",
                subject=subject,
                feature="relative_strength_ratio",
                value=float(ratio.iloc[-1]),
                unit="ratio",
                lookback=None,
                source="prices_daily",
            )
        )

        for window in config.return_windows:
            period_return = _trailing_return(
                ratio,
                window,
                subject,
                f"relative_strength_return_{window}d",
                warnings,
            )
            if period_return is None:
                continue
            rows.append(
                MarketFeatureRow(
                    as_of=as_of,
                    source_date=source_date,
                    category="relative_strength",
                    subject=subject,
                    feature=f"relative_strength_return_{window}d",
                    value=period_return,
                    unit="ratio",
                    lookback=window,
                    source="prices_daily",
                )
            )


def _append_vix_features(
    prices: pd.DataFrame,
    config: FeatureConfig,
    as_of: date,
    rows: list[MarketFeatureRow],
    warnings: list[FeatureWarning],
) -> None:
    history = prices.loc[prices["ticker"] == config.vix.ticker].sort_values("_date")
    if history.empty:
        warnings.append(
            FeatureWarning(
                code="vix_missing",
                message="VIX ticker is missing from prices",
                subject=config.vix.ticker,
                feature="vix_current",
                required_observations=1,
                available_observations=0,
            )
        )
        return

    latest = history.iloc[-1]
    source_date = _as_date(latest["_date"])
    latest_value = float(latest["_adj_close"])
    rows.append(
        MarketFeatureRow(
            as_of=as_of,
            source_date=source_date,
            category="risk_sentiment",
            subject=config.vix.ticker,
            feature="vix_current",
            value=latest_value,
            unit="index_level",
            lookback=None,
            source="prices_daily",
        )
    )

    ma = _trailing_mean(
        history["_adj_close"],
        config.vix.moving_average_window,
        config.vix.ticker,
        f"vix_ma_{config.vix.moving_average_window}",
        warnings,
    )
    if ma is not None:
        rows.append(
            MarketFeatureRow(
                as_of=as_of,
                source_date=source_date,
                category="risk_sentiment",
                subject=config.vix.ticker,
                feature=f"vix_ma_{config.vix.moving_average_window}",
                value=ma,
                unit="index_level",
                lookback=config.vix.moving_average_window,
                source="prices_daily",
            )
        )

    percentile = _trailing_percentile(
        history["_adj_close"],
        config.vix.percentile_window,
        config.vix.ticker,
        f"vix_percentile_{config.vix.percentile_window}",
        warnings,
    )
    if percentile is not None:
        rows.append(
            MarketFeatureRow(
                as_of=as_of,
                source_date=source_date,
                category="risk_sentiment",
                subject=config.vix.ticker,
                feature=f"vix_percentile_{config.vix.percentile_window}",
                value=percentile,
                unit="percentile",
                lookback=config.vix.percentile_window,
                source="prices_daily",
            )
        )


def _append_rate_features(
    rates: pd.DataFrame,
    config: FeatureConfig,
    as_of: date,
    rows: list[MarketFeatureRow],
    warnings: list[FeatureWarning],
) -> None:
    for series, group in rates.groupby("series", sort=True):
        history = group.sort_values("_date").reset_index(drop=True)
        if history.empty:
            continue

        latest = history.iloc[-1]
        source_date = _as_date(latest["_date"])
        latest_value = float(latest["_value"])
        rows.append(
            MarketFeatureRow(
                as_of=as_of,
                source_date=source_date,
                category="macro_liquidity",
                subject=str(series),
                feature="rate_current",
                value=latest_value,
                unit="percent",
                lookback=None,
                source="rates_daily",
            )
        )

        for window in config.rates.change_windows:
            change = _trailing_difference(
                history["_value"],
                window,
                str(series),
                f"rate_change_{window}d",
                warnings,
            )
            if change is None:
                continue
            rows.append(
                MarketFeatureRow(
                    as_of=as_of,
                    source_date=source_date,
                    category="macro_liquidity",
                    subject=str(series),
                    feature=f"rate_change_{window}d",
                    value=change,
                    unit="percentage_points",
                    lookback=window,
                    source="rates_daily",
                )
            )


def _append_core_breadth_features(
    prices: pd.DataFrame,
    config: FeatureConfig,
    core_watchlist: list[str],
    as_of: date,
    rows: list[MarketFeatureRow],
    warnings: list[FeatureWarning],
) -> None:
    window = config.core_breadth.long_moving_average_window
    above_count = 0
    evaluated_count = 0
    source_dates: list[date] = []

    for ticker in core_watchlist:
        history = prices.loc[prices["ticker"] == ticker].sort_values("_date").reset_index(drop=True)
        if history.empty:
            warnings.append(
                FeatureWarning(
                    code="core_breadth_missing_ticker",
                    message="core watchlist ticker is missing from prices",
                    subject=ticker,
                    feature=f"core_above_ma_{window}",
                    required_observations=window,
                    available_observations=0,
                )
            )
            continue

        ma = _trailing_mean(
            history["_adj_close"],
            window,
            ticker,
            f"core_above_ma_{window}",
            warnings,
        )
        if ma is None:
            continue

        latest = history.iloc[-1]
        latest_value = float(latest["_adj_close"])
        source_dates.append(_as_date(latest["_date"]))
        evaluated_count += 1
        if latest_value > ma:
            above_count += 1

    if evaluated_count == 0:
        warnings.append(
            FeatureWarning(
                code="core_breadth_no_evaluable_tickers",
                message="no core watchlist tickers had enough history for breadth calculation",
                subject="AI_CORE_WATCHLIST",
                feature=f"above_ma_{window}_ratio",
                required_observations=window,
                available_observations=0,
            )
        )
        return

    source_date = max(source_dates)
    rows.extend(
        [
            MarketFeatureRow(
                as_of=as_of,
                source_date=source_date,
                category="trend",
                subject="AI_CORE_WATCHLIST",
                feature=f"above_ma_{window}_ratio",
                value=above_count / evaluated_count,
                unit="ratio",
                lookback=window,
                source="prices_daily",
                notes=f"evaluated={evaluated_count}; total={len(core_watchlist)}",
            ),
            MarketFeatureRow(
                as_of=as_of,
                source_date=source_date,
                category="trend",
                subject="AI_CORE_WATCHLIST",
                feature=f"above_ma_{window}_count",
                value=float(above_count),
                unit="count",
                lookback=window,
                source="prices_daily",
                notes=f"evaluated={evaluated_count}; total={len(core_watchlist)}",
            ),
        ]
    )


def _trailing_mean(
    values: pd.Series,
    window: int,
    subject: str,
    feature: str,
    warnings: list[FeatureWarning],
) -> float | None:
    if len(values) < window:
        warnings.append(_insufficient_history_warning(subject, feature, window, len(values)))
        return None
    return float(values.tail(window).mean())


def _trailing_return(
    values: pd.Series,
    window: int,
    subject: str,
    feature: str,
    warnings: list[FeatureWarning],
) -> float | None:
    required_observations = window + 1
    if len(values) < required_observations:
        warnings.append(
            _insufficient_history_warning(subject, feature, required_observations, len(values))
        )
        return None
    previous = float(values.iloc[-required_observations])
    current = float(values.iloc[-1])
    if previous == 0:
        warnings.append(
            FeatureWarning(
                code="zero_return_base",
                message="return base value is zero",
                subject=subject,
                feature=feature,
                required_observations=required_observations,
                available_observations=len(values),
            )
        )
        return None
    return (current / previous) - 1.0


def _trailing_difference(
    values: pd.Series,
    window: int,
    subject: str,
    feature: str,
    warnings: list[FeatureWarning],
) -> float | None:
    required_observations = window + 1
    if len(values) < required_observations:
        warnings.append(
            _insufficient_history_warning(subject, feature, required_observations, len(values))
        )
        return None
    return float(values.iloc[-1]) - float(values.iloc[-required_observations])


def _trailing_percentile(
    values: pd.Series,
    window: int,
    subject: str,
    feature: str,
    warnings: list[FeatureWarning],
) -> float | None:
    if len(values) < window:
        warnings.append(_insufficient_history_warning(subject, feature, window, len(values)))
        return None
    window_values = values.tail(window)
    current = float(window_values.iloc[-1])
    return float((window_values <= current).sum() / len(window_values))


def _insufficient_history_warning(
    subject: str,
    feature: str,
    required_observations: int,
    available_observations: int,
) -> FeatureWarning:
    return FeatureWarning(
        code="insufficient_history",
        message="not enough observations to compute feature",
        subject=subject,
        feature=feature,
        required_observations=required_observations,
        available_observations=available_observations,
    )


def _as_date(value: object) -> date:
    return pd.Timestamp(cast(Any, value)).date()


def _count_by(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
