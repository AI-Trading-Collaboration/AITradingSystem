"""Option proxy probe (TRADING-2567-20): sell-volatility structures priced from the VXN.

A measurement on the development window only (2001-02-02 to 2010-12-31). It prices a monthly
cash-secured at-the-money put with Black-Scholes, using the VXN (scaled) as the volatility, and
compares it with a constant mix of the Nasdaq-100 and cash that has the same average exposure, and
with the same daily exposure held in the Nasdaq-100 and cash (the option's delta, without its
gamma, vega and decay). Without dividends a covered call is the same position by put-call parity,
so it is priced only as a check on the model (``parity``), not reported as a second structure. It
is not a verdict and not tradable: no bid-ask spread, no skew, no dividends, no early exercise, no
transaction cost, and the VXN is not an at-the-money volatility. Its use is to show how large the
premium is and how its losses are shaped before any option data is bought.

Cycle: opened at the close of the last trading date of a month, expires at the close of the last
trading date of the next month. Strike is the spot at opening. Between those dates the option is
marked daily with the day's scaled VXN and the T-bill rate known at that close. Cash and the
premium earn the T-bill return. Exposure held during a day is the position's Nasdaq-100 delta per
unit of wealth at the previous close.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal

import numpy as np
import pandas as pd
import typer

from ai_trading_system.research_bench.data import BenchData, QqqSeries, from_snapshot
from ai_trading_system.research_bench.falsify import bootstrap_interval, paired_comparison
from ai_trading_system.research_bench.metrics import summarize
from ai_trading_system.research_bench.settings import LoadedSettings, load_settings
from ai_trading_system.research_bench.simulate import Simulation
from ai_trading_system.research_data import qqq_long_history as qlh

DEV_START = date(2001, 2, 2)
DEV_END = date(2010, 12, 31)
TRADING_DAYS = 252
MAX_FILL_DAYS = 3
MIN_CYCLE_DAYS = 12
MONTH_END_TOLERANCE_DAYS = 4
MAX_GAP_DAYS = 10
RATE_RANGE = (-0.05, 0.5)
SIGMA_SCALES: tuple[float, ...] = (1.0, 0.9, 0.8)
CENTRAL_SIGMA_SCALE = 0.9
WORST_CYCLES = 5
Kind = Literal["put", "call"]
Structure = Literal["put_write", "covered_call"]
STRUCTURES: tuple[Structure, ...] = ("put_write", "covered_call")
REPORTED_STRUCTURE: Structure = "put_write"
REPLICA_COMPARED: tuple[str, ...] = ("geometric_annual_return", "max_drawdown", "cvar_5pct")
SUMMARY_KEYS: tuple[str, ...] = (
    "geometric_annual_return",
    "annual_volatility",
    "max_drawdown",
    "cvar_1pct",
    "cvar_5pct",
    "worst_month",
    "average_exposure",
    "episodes",
)

app = typer.Typer(help="期权代理测量（TRADING-2567-20）：只算开发窗口，输出只有汇总。")


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _check_option(kind: str, spot: float, strike: float, sigma: float, years: float) -> None:
    if kind not in ("call", "put"):
        raise ValueError("kind must be call or put")
    if not (spot > 0.0 and strike > 0.0 and math.isfinite(spot) and math.isfinite(strike)):
        raise ValueError("spot and strike must be positive and finite")
    if not (sigma >= 0.0 and math.isfinite(sigma)):
        raise ValueError("sigma must be finite and not negative")
    if not (years >= 0.0 and math.isfinite(years)):
        raise ValueError("years must be finite and not negative")


def _d1(spot: float, strike: float, rate: float, sigma: float, years: float) -> float:
    spread = sigma * math.sqrt(years)
    return (math.log(spot / strike) + (rate + 0.5 * sigma * sigma) * years) / spread


def bs_price(
    kind: Kind, spot: float, strike: float, rate: float, sigma: float, years: float
) -> float:
    """Black-Scholes price without dividends. At expiry the intrinsic value; with no volatility
    the intrinsic value against the discounted strike."""

    _check_option(kind, spot, strike, sigma, years)
    discounted = strike * math.exp(-rate * years)
    if years == 0.0 or sigma == 0.0:
        intrinsic = spot - (strike if years == 0.0 else discounted)
        return max(intrinsic, 0.0) if kind == "call" else max(-intrinsic, 0.0)
    d1 = _d1(spot, strike, rate, sigma, years)
    d2 = d1 - sigma * math.sqrt(years)
    if kind == "call":
        return spot * normal_cdf(d1) - discounted * normal_cdf(d2)
    return discounted * normal_cdf(-d2) - spot * normal_cdf(-d1)


def bs_delta(
    kind: Kind, spot: float, strike: float, rate: float, sigma: float, years: float
) -> float:
    """Derivative of the price with respect to the spot."""

    _check_option(kind, spot, strike, sigma, years)
    if years == 0.0 or sigma == 0.0:
        threshold = strike if years == 0.0 else strike * math.exp(-rate * years)
        if kind == "call":
            return 1.0 if spot > threshold else 0.0
        return -1.0 if spot < threshold else 0.0
    call = normal_cdf(_d1(spot, strike, rate, sigma, years))
    return call if kind == "call" else call - 1.0


@dataclass(frozen=True)
class ProxyInputs:
    """Daily inputs on the development window. ``vxn`` is percent per year; ``rate`` is a decimal
    per year (0.04 is 4%), as the bench's ``cash_rate_known`` is."""

    ndx_return: pd.Series
    cash_return: pd.Series
    vxn: pd.Series
    rate: pd.Series
    filled_days: dict[str, int]

    def __post_init__(self) -> None:
        index = pd.DatetimeIndex(self.vxn.index)
        for name in ("ndx_return", "cash_return", "rate"):
            if not getattr(self, name).index.equals(index):
                raise ValueError(f"{name} must have the same dates as vxn")
        if len(index) < 2 or not index.is_monotonic_increasing or index.has_duplicates:
            raise ValueError("dates must be unique, increasing and at least two")
        if index[0].date() < DEV_START or index[-1].date() > DEV_END:
            raise ValueError(f"dates must lie within {DEV_START} and {DEV_END}")
        for name in ("ndx_return", "cash_return", "vxn", "rate"):
            values = getattr(self, name).to_numpy(dtype=float)
            if not np.isfinite(values).all():
                raise ValueError(f"{name} has missing or infinite values")
        if (self.vxn.to_numpy(dtype=float) <= 0.0).any():
            raise ValueError("vxn must be positive")
        rates = self.rate.to_numpy(dtype=float)
        if rates.min() < RATE_RANGE[0] or rates.max() > RATE_RANGE[1]:
            raise ValueError(f"rate must be a decimal per year within {RATE_RANGE}, not percent")
        if (np.diff(index.to_numpy()) > np.timedelta64(MAX_GAP_DAYS, "D")).any():
            raise ValueError(f"dates have a gap longer than {MAX_GAP_DAYS} days")

    @classmethod
    def from_frames(
        cls, ndx_return: pd.Series, cash_return: pd.Series, vxn: pd.Series, rate: pd.Series
    ) -> ProxyInputs:
        """Cut the development window, then fill short gaps of vxn and rate with the last value
        (at most ``MAX_FILL_DAYS`` in a row, counted); a longer gap or a missing return raises."""

        window = pd.DatetimeIndex(vxn.index)
        keep = (window >= pd.Timestamp(DEV_START)) & (window <= pd.Timestamp(DEV_END))
        filled: dict[str, int] = {}
        cut: dict[str, pd.Series] = {}
        for name, series in (("vxn", vxn), ("rate", rate)):
            part = series.loc[keep].astype(float)
            missing = int(part.isna().sum())
            part = part.ffill(limit=MAX_FILL_DAYS)
            if part.isna().any():
                raise ValueError(f"{name} has gaps longer than {MAX_FILL_DAYS} days")
            filled[name] = missing
            cut[name] = part
        return cls(
            ndx_return=ndx_return.loc[keep].astype(float),
            cash_return=cash_return.loc[keep].astype(float),
            vxn=cut["vxn"],
            rate=cut["rate"],
            filled_days=filled,
        )

    @classmethod
    def from_bench(cls, data: BenchData) -> ProxyInputs:
        return cls.from_frames(
            data.inputs["ndx_return"],
            data.returns["cash"],
            data.inputs["vxn"],
            data.inputs["cash_rate_known"],
        )


@dataclass(frozen=True)
class Cycle:
    """One month-end cycle. ``premium`` and ``cycle_return`` are per unit of strike; volatilities
    are percent per year; ``premium_vol`` is the volatility priced at opening (the sigma scale times
    the VXN) minus the realized volatility."""

    start: pd.Timestamp
    end: pd.Timestamp
    days: int
    vxn_start: float
    premium: float
    underlying_return: float
    cycle_return: float
    realized_vol: float
    premium_vol: float


@dataclass(frozen=True)
class ProxyPath:
    """One structure over all cycles: daily returns, the Nasdaq-100 exposure held each day, and
    the cycles."""

    returns: pd.Series
    exposure: pd.Series
    cycles: list[Cycle]


def _near_month_end(day: pd.Timestamp) -> bool:
    return (day + pd.offsets.MonthEnd(0) - day).days <= MONTH_END_TOLERANCE_DAYS


def cycle_bounds(index: pd.DatetimeIndex) -> list[tuple[int, int]]:
    """Positions (open, expiry) of consecutive month-end cycles. A month end is the last date of a
    month in the data; the last date counts only when it lies within ``MONTH_END_TOLERANCE_DAYS``
    calendar days of its month's end (a holiday can close the last weekday)."""

    ends = [i for i in range(len(index) - 1) if index[i].month != index[i + 1].month]
    if _near_month_end(index[-1]):
        ends.append(len(index) - 1)
    return list(zip(ends[:-1], ends[1:], strict=True))


def _check_cycles(index: pd.DatetimeIndex, bounds: list[tuple[int, int]]) -> None:
    for start, end in bounds:
        for position in (start, end):
            if not _near_month_end(index[position]):
                raise ValueError(f"cycle date {index[position].date()} is not near a month end")
        if end - start < MIN_CYCLE_DAYS:
            raise ValueError(
                f"cycle {index[start].date()} to {index[end].date()} has only {end - start} "
                f"trading days (at least {MIN_CYCLE_DAYS} are required)"
            )


def structure_path(inputs: ProxyInputs, structure: Structure, sigma_scale: float) -> ProxyPath:
    if structure not in STRUCTURES:
        raise ValueError(f"structure must be one of {STRUCTURES}")
    if not (sigma_scale > 0.0 and math.isfinite(sigma_scale)):
        raise ValueError("sigma_scale must be positive and finite")
    index = pd.DatetimeIndex(inputs.vxn.index)
    ndx = inputs.ndx_return.to_numpy(dtype=float)
    cash = inputs.cash_return.to_numpy(dtype=float)
    sigma = sigma_scale * inputs.vxn.to_numpy(dtype=float) / 100.0
    rate = inputs.rate.to_numpy(dtype=float)
    kind: Kind = "put" if structure == "put_write" else "call"
    bounds = cycle_bounds(index)
    if not bounds:
        raise ValueError("the data holds no complete month-end cycle")
    _check_cycles(index, bounds)
    day_returns: list[float] = []
    day_exposure: list[float] = []
    day_index: list[pd.Timestamp] = []
    cycles: list[Cycle] = []
    for start, end in bounds:
        positions = np.arange(start, end + 1)
        spot = np.concatenate([[1.0], np.cumprod(1.0 + ndx[start + 1 : end + 1])])
        grown = np.concatenate([[1.0], np.cumprod(1.0 + cash[start + 1 : end + 1])])
        years = (end - positions) / TRADING_DAYS
        value = np.array(
            [
                bs_price(kind, spot[k], 1.0, rate[p], sigma[p], years[k])
                for k, p in enumerate(positions)
            ]
        )
        premium = float(value[0])
        if structure == "put_write":
            wealth = (1.0 + premium) * grown - value
        else:
            wealth = spot + premium * grown - value
        for k in range(1, len(positions)):
            p = positions[k - 1]
            delta = bs_delta(kind, spot[k - 1], 1.0, rate[p], sigma[p], years[k - 1])
            held = (-delta if structure == "put_write" else 1.0 - delta) * spot[k - 1]
            day_returns.append(float(wealth[k] / wealth[k - 1] - 1.0))
            day_exposure.append(float(held / wealth[k - 1]))
            day_index.append(index[positions[k]])
        squares = ndx[start + 1 : end + 1] ** 2
        realized = 100.0 * math.sqrt(TRADING_DAYS * float(squares.mean()))
        implied = float(inputs.vxn.iloc[start])
        cycles.append(
            Cycle(
                start=index[start],
                end=index[end],
                days=end - start,
                vxn_start=implied,
                premium=premium,
                underlying_return=float(spot[-1] - 1.0),
                cycle_return=float(wealth[-1] - 1.0),
                realized_vol=realized,
                premium_vol=sigma_scale * implied - realized,
            )
        )
    dates = pd.DatetimeIndex(day_index)
    return ProxyPath(
        returns=pd.Series(day_returns, index=dates),
        exposure=pd.Series(day_exposure, index=dates),
        cycles=cycles,
    )


def control_returns(inputs: ProxyInputs, path: ProxyPath) -> tuple[pd.Series, float]:
    """A constant mix of the Nasdaq-100 and cash, rebalanced daily, at the path's mean exposure."""

    mean = float(path.exposure.mean())
    dates = path.returns.index
    mixed = mean * inputs.ndx_return.loc[dates] + (1.0 - mean) * inputs.cash_return.loc[dates]
    return mixed, mean


def replica_returns(inputs: ProxyInputs, path: ProxyPath) -> pd.Series:
    """The path's own daily exposure held in the Nasdaq-100 with the rest in cash: the option's
    delta without its gamma, vega and time decay."""

    dates = path.returns.index
    held = path.exposure
    return held * inputs.ndx_return.loc[dates] + (1.0 - held) * inputs.cash_return.loc[dates]


def _simulation(returns: pd.Series, exposure: pd.Series, start: pd.Timestamp) -> Simulation:
    daily = pd.DataFrame(
        {
            "return": returns.to_numpy(dtype=float),
            "exposure": exposure.to_numpy(dtype=float),
            "turnover": 0.0,
            "cost": 0.0,
        },
        index=returns.index,
    )
    return Simulation(daily=daily, start=start)


def _summary(sim: Simulation, cash: pd.Series, loaded: LoadedSettings) -> dict[str, Any]:
    full = summarize(sim, cash, loaded)
    return {key: full[key] for key in SUMMARY_KEYS}


def _with_draws(loaded: LoadedSettings, draws: int | None) -> LoadedSettings:
    if draws is None:
        return loaded
    falsification = loaded.settings.falsification.model_copy(update={"bootstrap_draws": draws})
    return replace(
        loaded, settings=loaded.settings.model_copy(update={"falsification": falsification})
    )


def _variant(
    inputs: ProxyInputs, structure: Structure, scale: float, loaded: LoadedSettings
) -> dict[str, Any]:
    path = structure_path(inputs, structure, scale)
    control, mean = control_returns(inputs, path)
    replica = replica_returns(inputs, path)
    dates = path.returns.index
    start = path.cycles[0].start
    cash = inputs.cash_return.loc[dates]
    strategy_sim = _simulation(path.returns, path.exposure, start)
    control_sim = _simulation(control, pd.Series(mean, index=dates), start)
    replica_sim = _simulation(replica, path.exposure, start)
    strategy_summary = _summary(strategy_sim, cash, loaded)
    control_summary = _summary(control_sim, cash, loaded)
    replica_summary = _summary(replica_sim, cash, loaded)
    strategy_array = path.returns.to_numpy(dtype=float)
    control_array = control.to_numpy(dtype=float)
    cash_array = cash.to_numpy(dtype=float)
    low, high = bootstrap_interval(
        strategy_array, control_array, cash_array, loaded, loaded.settings.falsification.confidence
    )
    cycles = path.cycles
    worst = sorted(cycles, key=lambda cycle: cycle.cycle_return)[:WORST_CYCLES]
    return {
        "sigma_scale": scale,
        "mean_exposure": mean,
        "strategy": strategy_summary,
        "control": control_summary,
        "geometric_annual_difference": (
            strategy_summary["geometric_annual_return"] - control_summary["geometric_annual_return"]
        ),
        "replica": replica_summary,
        "strategy_vs_replica": {
            key: strategy_summary[key] - replica_summary[key] for key in REPLICA_COMPARED
        },
        "paired": paired_comparison(strategy_array, control_array, cash_array, loaded),
        "ci_low": low,
        "ci_high": high,
        "confidence": loaded.settings.falsification.confidence,
        "cycles": len(cycles),
        "mean_premium": float(np.mean([cycle.premium for cycle in cycles])),
        "premium_vol": premium_summary(cycles),
        "share_cycles_positive": float(np.mean([cycle.cycle_return > 0.0 for cycle in cycles])),
        "worst_cycles": [
            {"end": cycle.end.strftime("%Y-%m"), "cycle_return": cycle.cycle_return}
            for cycle in worst
        ],
    }


def premium_summary(cycles: list[Cycle]) -> dict[str, Any]:
    """Priced minus realized volatility, in percentage points, over the cycles."""

    values = np.array([cycle.premium_vol for cycle in cycles], dtype=float)
    by_year: dict[str, list[float]] = {}
    for cycle in cycles:
        by_year.setdefault(str(cycle.end.year), []).append(cycle.premium_vol)
    worst = min(cycles, key=lambda cycle: cycle.premium_vol)
    return {
        "cycles": len(cycles),
        "mean_vol_points": float(values.mean()),
        "median_vol_points": float(np.median(values)),
        "share_positive": float((values > 0.0).mean()),
        "worst_cycle": {"end": worst.end.strftime("%Y-%m"), "vol_points": worst.premium_vol},
        "by_year": {year: float(np.mean(items)) for year, items in sorted(by_year.items())},
    }


def _parity(inputs: ProxyInputs, scale: float) -> dict[str, float]:
    """Put-write against covered call: the same position without dividends, so any difference is
    the model's (the T-bill rate used in the price against the cash return that is earned)."""

    put = structure_path(inputs, "put_write", scale).returns.to_numpy(dtype=float)
    call = structure_path(inputs, "covered_call", scale).returns.to_numpy(dtype=float)
    return {
        "max_abs_daily_difference": float(np.abs(put - call).max()),
        "wealth_ratio_minus_one": float(np.prod(1.0 + put) / np.prod(1.0 + call) - 1.0),
    }


def run_probe(
    inputs: ProxyInputs,
    loaded: LoadedSettings | None = None,
    *,
    scales: tuple[float, ...] = SIGMA_SCALES,
    draws: int | None = None,
) -> dict[str, Any]:
    loaded = _with_draws(loaded or load_settings(), draws)
    index = pd.DatetimeIndex(inputs.vxn.index)
    bounds = cycle_bounds(index)
    reference = structure_path(inputs, "put_write", 1.0)
    result: dict[str, Any] = {
        "probe": "option_proxy",
        "task": "TRADING-2567-20",
        "window": {
            "start": index[bounds[0][0]].date().isoformat(),
            "end": index[bounds[-1][1]].date().isoformat(),
            "cycles": len(bounds),
            "return_days": int(len(reference.returns)),
        },
        "assumptions": {
            "pricing": "black_scholes_no_dividends",
            "volatility": "vxn_times_sigma_scale",
            "sigma_scales": list(scales),
            "central_sigma_scale": CENTRAL_SIGMA_SCALE,
            "strike": "spot_at_opening",
            "expiry": "last_trading_date_of_next_month",
            "marking": "daily_model_value",
            "control": "constant_mix_rebalanced_daily_at_mean_exposure",
            "replica": "same_daily_exposure_held_in_the_nasdaq_100_and_cash",
            "paired_annualisation": "paired_and_ci_use_the_252_day_log_mean",
            "bootstrap_mean_block_days": loaded.settings.falsification.bootstrap_mean_block_days,
            "bootstrap_draws": loaded.settings.falsification.bootstrap_draws,
            "costs": "none",
        },
        "inputs": {
            "mean_vxn": float(inputs.vxn.mean()),
            "mean_rate": float(inputs.rate.mean()),
        },
        "filled_days": dict(inputs.filled_days),
        "premium": premium_summary(reference.cycles),
        "structures": {
            REPORTED_STRUCTURE: {
                f"{scale:g}": _variant(inputs, REPORTED_STRUCTURE, scale, loaded)
                for scale in scales
            }
        },
        "parity": {f"{scale:g}": _parity(inputs, scale) for scale in scales},
    }
    cleaned: dict[str, Any] = clean(result)
    return cleaned


def clean(value: Any) -> Any:
    """Plain JSON values: numpy numbers become floats, NaN and infinity become None."""

    if isinstance(value, dict):
        return {str(key): clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    if isinstance(value, (bool, str)) or value is None:
        return value
    if isinstance(value, (int, np.integer)):
        return int(value)
    number = float(value)
    return number if math.isfinite(number) else None


def _pct(value: Any, digits: int = 1) -> str:
    return "n/a" if value is None else f"{100.0 * float(value):.{digits}f}%"


def _interval(row: dict[str, Any], key: str) -> str:
    low, high = row["ci_low"][key], row["ci_high"][key]
    return f"{_pct(row['paired'][key])} [{_pct(low)}, {_pct(high)}]"


def render_markdown(result: dict[str, Any]) -> str:
    window = result["window"]
    mean = result["inputs"]
    lines = [
        "# 期权代理测量（TRADING-2567-20，只含开发窗口）",
        "",
        f"窗口 {window['start']} 到 {window['end']}，{window['cycles']} 个月度周期，"
        f"{window['return_days']} 个交易日；平均 VXN {mean['mean_vxn']:.1f}，"
        f"平均利率 {_pct(mean['mean_rate'], 2)}。BS 定价、VXN 乘以缩放系数当波动率，"
        "不含价差、偏度、分红、成本；对照组是同平均敞口的固定仓位（每天再平衡）。",
        "",
        "## 溢价：VXN 减已实现波动（百分点）",
        "",
    ]
    premium = result["premium"]
    lines.append(
        f"均值 {premium['mean_vol_points']:.2f}，中位数 {premium['median_vol_points']:.2f}，"
        f"为正的周期占 {_pct(premium['share_positive'])}，"
        f"最差周期 {premium['worst_cycle']['end']}（{premium['worst_cycle']['vol_points']:.2f}）。"
    )
    lines.append("")
    lines.append("| 年 | " + " | ".join(premium["by_year"]) + " |")
    lines.append("|---|" + "---|" * len(premium["by_year"]))
    lines.append("| 均值 | " + " | ".join(f"{v:.1f}" for v in premium["by_year"].values()) + " |")
    for structure, variants in result["structures"].items():
        lines += ["", f"## {structure}", ""]
        lines.append(
            "| sigma_scale | 平均敞口 | 几何年化 策略/对照 | 几何年化差 | 最大回撤 策略/对照 |"
            " 回撤差 [区间] | CVaR5 相对改善 [区间] | 卖出波动减已实现(点) | 为正周期 |"
        )
        lines.append("|---|---|---|---|---|---|---|---|---|")
        for scale, row in variants.items():
            strategy, control = row["strategy"], row["control"]
            geometric = "geometric_annual_return"
            drawdown = "max_drawdown"
            tail = "cvar_5pct_relative_improvement"
            lines.append(
                f"| {scale} | {row['mean_exposure']:.2f}"
                f" | {_pct(strategy[geometric])} / {_pct(control[geometric])}"
                f" | {_pct(row['geometric_annual_difference'])}"
                f" | {_pct(strategy[drawdown])} / {_pct(control[drawdown])}"
                f" | {_interval(row, drawdown)} | {_interval(row, tail)}"
                f" | {row['premium_vol']['mean_vol_points']:.2f}"
                f" | {_pct(row['share_cycles_positive'])} |"
            )
        for scale, row in variants.items():
            replica, gap = row["replica"], row["strategy_vs_replica"]
            lines.append("")
            lines.append(
                f"sigma_scale {scale} 同日敞口对照（只持有标的和现金）：几何年化 "
                f"{_pct(replica['geometric_annual_return'])}、"
                f"最大回撤 {_pct(replica['max_drawdown'])}、"
                f"CVaR5 {_pct(replica['cvar_5pct'], 2)}；策略减对照：几何年化 "
                f"{_pct(gap['geometric_annual_return'])}、"
                f"最大回撤 {_pct(gap['max_drawdown'])}、CVaR5 {_pct(gap['cvar_5pct'], 2)}"
            )
            worst = "，".join(f"{w['end']} {_pct(w['cycle_return'])}" for w in row["worst_cycles"])
            lines.append("")
            lines.append(f"sigma_scale {scale} 最差的月度周期：{worst}")
            for name, episode in row["strategy"]["episodes"].items():
                control_episode = row["control"]["episodes"].get(name, {})
                lines.append(
                    f"- 区间 {name}：策略 {_pct(episode['return'])}，"
                    f"对照 {_pct(control_episode.get('return'))}"
                )
    lines += ["", "## 核对：covered call 与卖 put 在没有分红时是同一仓位", ""]
    for scale, check in result["parity"].items():
        lines.append(
            f"- sigma_scale {scale}：逐日收益最大差 {check['max_abs_daily_difference']:.2e}，"
            f"累计财富比减 1 为 {check['wealth_ratio_minus_one']:.2e}"
        )
    return "\n".join(lines) + "\n"


def write_result(result: dict[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "probe_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    (out_dir / "probe_result.md").write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """期权代理测量。"""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，写 probe_result.json 和 probe_result.md。")],
    snapshot: Annotated[str, typer.Option(help="长历史快照 ID，默认 LATEST。")] = "",
    root: Annotated[Path, typer.Option(help="快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
    qqq_series: Annotated[str, typer.Option(help="QQQ 用哪条序列：proxy 或 actual。")] = "actual",
    draws: Annotated[int | None, typer.Option(help="自助法次数，默认用评估台设置。")] = None,
) -> None:
    """读开发窗口，算 put-write 和 covered call 的代理，写汇总。"""

    if qqq_series not in ("proxy", "actual"):
        typer.echo("option_proxy: --qqq-series must be proxy or actual")
        raise typer.Exit(code=1)
    series: QqqSeries = "proxy" if qqq_series == "proxy" else "actual"
    data = from_snapshot(qlh.resolve_snapshot_dir(root, snapshot or None), qqq_series=series)
    result = run_probe(ProxyInputs.from_bench(data), load_settings(), draws=draws)
    result["source"] = {"snapshot_id": data.source["snapshot_id"], "qqq_series": series}
    write_result(result, out)
    typer.echo(
        f"snapshot {data.source['snapshot_id']} qqq {series}; "
        f"{result['window']['cycles']} cycles; wrote {out / 'probe_result.json'}"
    )


if __name__ == "__main__":
    app()
