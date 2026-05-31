from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ai_trading_system.etf_portfolio.models import ETFAssetsConfig, ETFStrategyConfig

FEATURE_VERSION = "etf_features_v0_1"


def build_feature_store(
    prices: pd.DataFrame,
    *,
    assets: ETFAssetsConfig,
    strategy: ETFStrategyConfig,
    start: date | None = None,
    end: date | None = None,
) -> pd.DataFrame:
    frame = _prepare_prices(prices, start=start, end=end)
    if frame.empty:
        raise ValueError("ETF feature store cannot be built from empty price data")

    windows = _feature_windows(strategy)
    rows: list[pd.DataFrame] = []
    for symbol, group in frame.groupby("symbol", sort=True):
        history = group.sort_values("_date").reset_index(drop=True)
        enriched = pd.DataFrame(
            {
                "date": history["_date"].dt.date.astype(str),
                "symbol": symbol,
                "close": history["_close"],
                "adj_close": history["_adj_close"],
                "volume": history["_volume"],
            }
        )
        for window in sorted(windows["returns"]):
            enriched[f"ret_{window}d"] = history["_adj_close"].pct_change(window)
        for window in sorted(windows["ma"]):
            ma = history["_adj_close"].rolling(window=window, min_periods=window).mean()
            enriched[f"ma_{window}"] = ma
            enriched[f"above_ma_{window}"] = history["_adj_close"] > ma
            enriched[f"pct_vs_ma_{window}"] = (history["_adj_close"] / ma) - 1.0
        slope_window = strategy.trend_features.slope_window
        if "ma_20" in enriched.columns:
            enriched["ma_20_slope"] = enriched["ma_20"] / enriched["ma_20"].shift(
                slope_window
            ) - 1.0
        for window in sorted(windows["vol"]):
            enriched[f"realized_vol_{window}d"] = (
                history["_adj_close"].pct_change().rolling(window=window, min_periods=window).std()
                * np.sqrt(252)
            )
        for window in sorted(windows["drawdown"]):
            rolling_max = history["_adj_close"].rolling(window=window, min_periods=window).max()
            enriched[f"drawdown_{window}d"] = history["_adj_close"] / rolling_max - 1.0
        rolling_volume_mean = history["_volume"].rolling(window=20, min_periods=20).mean()
        rolling_volume_std = history["_volume"].rolling(window=20, min_periods=20).std()
        volume_zscore = (history["_volume"] - rolling_volume_mean) / rolling_volume_std
        enriched["volume_zscore_20d"] = volume_zscore.replace([np.inf, -np.inf], np.nan)
        rows.append(enriched)

    features = pd.concat(rows, ignore_index=True)
    features = _append_relative_strength(features, frame, assets, strategy)
    features["created_at"] = datetime.now(UTC).isoformat()
    features["feature_version"] = FEATURE_VERSION
    return features.sort_values(["date", "symbol"]).reset_index(drop=True)


def write_feature_store(features: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    features.to_csv(path, index=False)
    return path


def load_feature_store(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"ETF feature store does not exist: {path}")
    return pd.read_csv(path)


def latest_feature_date(features: pd.DataFrame) -> date:
    if "date" not in features.columns:
        raise ValueError("ETF feature store missing date column")
    parsed = pd.to_datetime(features["date"], errors="coerce").dropna()
    if parsed.empty:
        raise ValueError("ETF feature store has no valid date")
    return parsed.max().date()


def select_features_for_date(features: pd.DataFrame, run_date: date) -> pd.DataFrame:
    frame = features.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[frame["_date"] == pd.Timestamp(run_date)].copy()
    if selected.empty:
        raise ValueError(f"ETF feature store has no rows for {run_date.isoformat()}")
    return selected.drop(columns=["_date"])


def _prepare_prices(
    prices: pd.DataFrame,
    *,
    start: date | None,
    end: date | None,
) -> pd.DataFrame:
    required = {"date", "symbol", "close", "adj_close", "volume"}
    missing = sorted(required - set(prices.columns))
    if missing:
        raise ValueError(f"ETF price data missing columns for feature store: {', '.join(missing)}")
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame["_volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0.0)
    frame = frame.loc[
        frame["_date"].notna() & frame["_close"].notna() & frame["_adj_close"].notna()
    ].copy()
    if start is not None:
        frame = frame.loc[frame["_date"] >= pd.Timestamp(start)].copy()
    if end is not None:
        frame = frame.loc[frame["_date"] <= pd.Timestamp(end)].copy()
    return frame.sort_values(["symbol", "_date"]).reset_index(drop=True)


def _feature_windows(strategy: ETFStrategyConfig) -> dict[str, set[int]]:
    return {
        "returns": set(
            [1, 5, *strategy.momentum_features.return_windows, *strategy.relative_strength.windows]
        ),
        "ma": set(strategy.trend_features.ma_windows),
        "vol": set(strategy.risk_features.vol_windows),
        "drawdown": set(strategy.risk_features.drawdown_windows),
    }


def _append_relative_strength(
    features: pd.DataFrame,
    prices: pd.DataFrame,
    assets: ETFAssetsConfig,
    strategy: ETFStrategyConfig,
) -> pd.DataFrame:
    output = features.copy()
    price_pivot = prices.pivot(index="_date", columns="symbol", values="_adj_close").sort_index()
    for denominator in ("SPY", "QQQ", "SMH"):
        for window in strategy.relative_strength.windows:
            output[f"rs_vs_{denominator.lower()}_{window}d"] = np.nan

    rs_frames: list[pd.DataFrame] = []
    configured_symbols = set(assets.assets) | {str(symbol) for symbol in price_pivot.columns}
    for symbol in sorted(configured_symbols):
        if symbol not in price_pivot.columns:
            continue
        symbol_frame = pd.DataFrame({"date": price_pivot.index.date.astype(str), "symbol": symbol})
        for denominator in ("SPY", "QQQ", "SMH"):
            if denominator not in price_pivot.columns:
                continue
            ratio = price_pivot[symbol] / price_pivot[denominator]
            for window in strategy.relative_strength.windows:
                symbol_frame[f"rs_vs_{denominator.lower()}_{window}d"] = (
                    ratio / ratio.shift(window) - 1.0
                ).to_numpy()
        rs_frames.append(symbol_frame)

    if not rs_frames:
        return output
    relative_strength = pd.concat(rs_frames, ignore_index=True)
    output = output.drop(
        columns=[
            column
            for column in output.columns
            if column.startswith("rs_vs_") and column.endswith("d")
        ],
        errors="ignore",
    )
    return output.merge(relative_strength, on=["date", "symbol"], how="left")
