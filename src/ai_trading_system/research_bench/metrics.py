"""Metrics: the risk side first (drawdown, left tail, compounding), then return and cost.

Every field is computed from the simulated daily returns and exposures; the invariant suite checks
that each one moves when the returns move, so no field can silently be a constant.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.research_bench.deflated import ReturnMoments
from ai_trading_system.research_bench.settings import BenchSettings, LoadedSettings
from ai_trading_system.research_bench.simulate import Simulation

DAYS_PER_YEAR = 365.25
# Comparisons keep these fields; a positive difference means the strategy is higher.
COMPARED_FIELDS: tuple[str, ...] = (
    "geometric_annual_return",
    "annual_volatility",
    "sharpe_vs_cash",
    "max_drawdown",
    "longest_drawdown_days",
    "worst_month",
    "rolling_geometric_return_low",
    "average_exposure",
    "annual_turnover",
    "annual_cost",
)


def summarize(sim: Simulation, cash_returns: pd.Series, loaded: LoadedSettings) -> dict[str, Any]:
    settings = loaded.settings
    returns = sim.returns.astype(float)
    if returns.empty:
        raise ValueError("simulation has no return dates")
    per_year = settings.trading_days_per_year
    equity = pd.concat([pd.Series([1.0], index=[sim.start]), (1.0 + returns).cumprod()])
    drawdown = equity / equity.cummax() - 1.0
    years = (returns.index[-1] - sim.start).days / DAYS_PER_YEAR
    excess = returns - cash_returns.reindex(returns.index).astype(float)
    summary: dict[str, Any] = {
        "first_return_date": returns.index[0].date().isoformat(),
        "last_return_date": returns.index[-1].date().isoformat(),
        "return_days": len(returns),
        "years": years,
        "geometric_annual_return": float(equity.iloc[-1]) ** (1.0 / years) - 1.0,
        "annual_volatility": _std(returns) * math.sqrt(per_year),
        "sharpe_vs_cash": _ratio(float(excess.mean()), _std(excess)) * math.sqrt(per_year),
        "max_drawdown": float(drawdown.min()),
        "longest_drawdown_days": _longest_run(drawdown.to_numpy() < 0.0),
        "worst_month": float(_monthly(returns).min()),
        "rolling_geometric_return_low": _rolling_low(returns, settings, per_year),
        "average_exposure": float(sim.exposure.mean()),
        "annual_turnover": float(sim.daily["turnover"].sum()) / years,
        "annual_cost": float(sim.daily["cost"].sum()) / years,
    }
    moments = excess_moments(excess)
    summary["sharpe_daily"] = moments.sharpe
    summary["excess_skewness"] = moments.skewness
    summary["excess_kurtosis"] = moments.kurtosis
    for level in settings.cvar_levels:
        summary[cvar_field(level)] = cvar(returns, level)
    summary["episodes"] = _episodes(returns, sim.start, loaded)
    return summary


def excess_moments(excess: pd.Series) -> ReturnMoments:
    """Daily Sharpe ratio (ddof 1), skewness and non-excess kurtosis of the daily excess returns:
    the inputs of the deflated Sharpe ratio. Undefined moments (no variation) are NaN."""

    values = excess.to_numpy(dtype=float)
    centered = values - values.mean()
    m2 = float(np.mean(centered**2))
    if m2 <= 0.0:
        return ReturnMoments(math.nan, math.nan, math.nan, len(values))
    return ReturnMoments(
        sharpe=_ratio(float(values.mean()), _std(excess)),
        skewness=float(np.mean(centered**3)) / m2**1.5,
        kurtosis=float(np.mean(centered**4)) / m2**2,
        observations=len(values),
    )


def cvar_field(level: float) -> str:
    return f"cvar_{round(level * 100, 4):g}pct"


def cvar(returns: pd.Series, level: float) -> float:
    """Mean of the worst ``level`` share of daily returns (at least one day)."""

    count = max(1, math.ceil(level * len(returns)))
    return float(np.sort(returns.to_numpy(dtype=float))[:count].mean())


def compare(
    strategy: dict[str, Any], benchmark: dict[str, Any], loaded: LoadedSettings
) -> dict[str, Any]:
    """Strategy minus benchmark for the compared fields, and the relative left-tail change
    (positive means the strategy's worst days are milder)."""

    fields = [*COMPARED_FIELDS, *(cvar_field(level) for level in loaded.settings.cvar_levels)]
    out: dict[str, Any] = {
        field: _difference(strategy.get(field), benchmark.get(field)) for field in fields
    }
    for level in loaded.settings.cvar_levels:
        field = cvar_field(level)
        out[f"{field}_relative_improvement"] = _ratio(
            float(strategy[field]) - float(benchmark[field]), abs(float(benchmark[field]))
        )
    return out


def exposure_gap(sim: Simulation, reference: float, qqq_returns: pd.Series) -> dict[str, float]:
    """Return given up or saved, per year, by holding less (or more) than ``reference``.

    Below the reference: ``missed_upside`` is the QQQ rise not earned, ``avoided_downside`` the QQQ
    fall not suffered. The negative-results list asks every de-risking scheme to show both.
    """

    exposure = sim.exposure
    qqq = qqq_returns.reindex(exposure.index).astype(float)
    years = (exposure.index[-1] - sim.start).days / DAYS_PER_YEAR
    below = (reference - exposure).clip(lower=0.0)
    above = (exposure - reference).clip(lower=0.0)
    up = qqq.clip(lower=0.0)
    down = (-qqq).clip(lower=0.0)
    return {
        "reference_exposure": float(reference),
        "missed_upside": float((below * up).sum()) / years,
        "avoided_downside": float((below * down).sum()) / years,
        "extra_upside": float((above * up).sum()) / years,
        "extra_downside": float((above * down).sum()) / years,
    }


def _episodes(returns: pd.Series, start: pd.Timestamp, loaded: LoadedSettings) -> dict[str, Any]:
    months = pd.DatetimeIndex(returns.index).to_period("M")
    first_held = start.to_period("M")
    last = months[-1]
    out: dict[str, Any] = {}
    for episode in loaded.settings.episodes:
        begin = pd.Period(episode.start, freq="M")
        finish = pd.Period(episode.end, freq="M")
        mask = (months >= begin) & (months <= finish)
        if not mask.any():
            continue
        out[episode.id] = {
            "return": float(np.prod(1.0 + returns.to_numpy(dtype=float)[mask])) - 1.0,
            "complete": bool(first_held < begin and last > finish),
        }
    return out


def _monthly(returns: pd.Series) -> pd.Series:
    return (1.0 + returns).groupby(pd.DatetimeIndex(returns.index).to_period("M")).prod() - 1.0


def _rolling_low(returns: pd.Series, settings: BenchSettings, per_year: int) -> float | None:
    window = int(settings.rolling_years * per_year)
    if len(returns) < window:
        return None
    logs = np.log1p(returns.to_numpy(dtype=float))
    sums = pd.Series(logs).rolling(window).sum().dropna().to_numpy()
    annual = np.expm1(sums * per_year / window)
    return float(np.quantile(annual, settings.rolling_percentile))


def _longest_run(flags: np.ndarray[Any, Any]) -> int:
    longest = current = 0
    for flag in flags:
        current = current + 1 if flag else 0
        longest = max(longest, current)
    return longest


def _std(series: pd.Series) -> float:
    return float(series.std(ddof=1)) if len(series) > 1 else 0.0


def _ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator > 0 else math.nan


def _difference(left: Any, right: Any) -> float | None:
    if left is None or right is None:
        return None
    return float(left) - float(right)
