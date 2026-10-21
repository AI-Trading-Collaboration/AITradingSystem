"""Development-only signal screening; summaries, not investment approval (TRADING-2565 S4b)."""

from __future__ import annotations

import importlib.util
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import NormalDist
from typing import Any, Literal, cast

import numpy as np
import pandas as pd
from pandas.api.types import is_complex_dtype, is_numeric_dtype
from pydantic import Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import checks
from ai_trading_system.research_bench.falsify import stationary_bootstrap_indices
from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.research_bench.screen_settings import ScreenRules, _Strict
from ai_trading_system.research_bench.sources import SourceSpec, align_source
from ai_trading_system.yaml_loader import safe_load_yaml_path

SIGNALS_DIR = PROJECT_ROOT / "src" / "ai_trading_system" / "research_screens" / "signals"
# Unit conversion, memory cap, and numerical degeneracy/rounding tolerances, not policy cutoffs.
TRADING_DAYS = 252
BOOTSTRAP_BATCH = 100
RESIDUAL_EPS = 1e-9
BREADTH_TOLERANCE = 1e-6  # Approved rounded representation of 2/3 in the screen policy.
Array = np.ndarray[Any, Any]
SignalFunction = Callable[[pd.Series, Mapping[str, pd.DataFrame], Mapping[str, Any]], pd.Series]


class ScreenCheckError(ValueError):
    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("; ".join(problems))


@dataclass(frozen=True)
class ScreenMarket:
    market_id: str
    returns: pd.Series
    eval_start: date


class SignalSpec(_Strict):
    signal_id: str = Field(pattern=r"^[a-z0-9_]+$")
    mechanism: str = Field(min_length=1)
    expected_sign: Literal[1, -1]
    target: Literal["return", "risk"]
    params: dict[str, Any]
    sources: tuple[str, ...]
    markets: Literal["all", "qqq"]
    literature: str = Field(min_length=1)

    @model_validator(mode="after")
    def nonblank(self) -> SignalSpec:
        if not self.mechanism.strip() or not self.literature.strip():
            raise ValueError("mechanism and literature must not be blank")
        if len(set(self.sources)) != len(self.sources):
            raise ValueError("sources must be unique")
        return self


@dataclass(frozen=True)
class LoadedSignal:
    signal_id: str
    spec: SignalSpec
    function: SignalFunction
    module_sha256: str
    spec_sha256: str


def load_signal(signal_id: str, directory: Path = SIGNALS_DIR) -> LoadedSignal:
    try:
        if not re.fullmatch(r"[a-z0-9_]+", signal_id):
            raise ValueError("invalid signal_id")
        module_path = directory / f"{signal_id}.py"
        spec_path = directory / f"{signal_id}.yaml"
        specification = SignalSpec.model_validate(safe_load_yaml_path(spec_path))
        if specification.signal_id != signal_id:
            raise ValueError("signal_id must equal the file name")
        module_sha = chain_digest(module_path.read_bytes())
        spec_sha = chain_digest(spec_path.read_bytes())
        module_spec = importlib.util.spec_from_file_location(
            f"aits_screen_{signal_id}_{module_sha[:12]}", module_path
        )
        if module_spec is None or module_spec.loader is None:
            raise ValueError("cannot load signal module")
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        function = getattr(module, "signal", None)
        if not callable(function):
            raise ValueError("module has no signal function")
        return LoadedSignal(signal_id, specification, function, module_sha, spec_sha)
    except Exception as exc:
        raise ScreenCheckError([f"signal {signal_id}: {exc}"]) from exc


def minimum_detectable_ic(n_eff: float) -> float:
    if not math.isfinite(n_eff) or n_eff <= 0:
        raise ValueError("n_eff must be positive and finite")
    normal = NormalDist()
    return (normal.inv_cdf(0.975) + normal.inv_cdf(0.80)) / math.sqrt(n_eff)


def bh_adjust(p_values: Sequence[float]) -> list[float]:
    p = np.asarray(p_values, dtype=float)
    if not np.isfinite(p).all() or ((p < 0) | (p > 1)).any():
        raise ValueError("p-values must be finite and in [0, 1]")
    if not len(p):
        return []
    order = np.argsort(p, kind="stable")
    adjusted = p[order] * len(p) / np.arange(1, len(p) + 1)
    adjusted = np.minimum(1, np.minimum.accumulate(adjusted[::-1])[::-1])
    out = np.empty(len(p))
    out[order] = adjusted
    return out.tolist()


def expanding_percentile(values: pd.Series, warmup: int) -> pd.Series:
    """Midrank relative to previous valid observations, including ties, in O(n log n).

    Coordinate compression only labels values; the Fenwick tree contains observations <= t.
    """
    if warmup < 1:
        raise ValueError("warmup must be positive")
    data = values.to_numpy(dtype=float)
    valid = np.isfinite(data)
    coordinates = np.unique(data[valid])
    tree = np.zeros(len(coordinates) + 1, dtype=np.int64)
    counts = np.zeros(len(coordinates), dtype=np.int64)
    out = np.full(len(data), np.nan)
    total = 0
    for t in np.flatnonzero(valid):
        k = int(np.searchsorted(coordinates, data[t]))
        below, position = 0, k
        while position:
            below += int(tree[position])
            position -= position & -position
        total += 1
        if total > warmup:
            out[t] = (below + 0.5 * counts[k]) / (total - 1)
        counts[k] += 1
        position = k + 1
        while position < len(tree):
            tree[position] += 1
            position += position & -position
    return pd.Series(out, index=values.index)


def baseline_signals(returns: pd.Series) -> dict[str, pd.Series]:
    logs = np.log1p(returns)
    price = np.exp(logs.cumsum())
    return {
        "mom_12_1": logs.rolling(231).sum().shift(21),
        "ma_10m_distance": price / price.rolling(210).mean() - 1,
        "rv_21": returns.rolling(21).std() * math.sqrt(TRADING_DAYS),
    }


def _forward_values(returns: pd.Series, horizon: int) -> pd.DataFrame:
    logs = np.log1p(returns)
    forward = logs.rolling(horizon).sum().shift(-horizon)
    vol = returns.rolling(horizon).std(ddof=1).shift(-horizon) * math.sqrt(TRADING_DAYS)
    dd = np.full(len(returns), np.nan)
    if len(returns) > horizon:
        windows = np.lib.stride_tricks.sliding_window_view(logs.to_numpy()[1:], horizon)
        path = np.column_stack([np.zeros(len(windows)), np.cumsum(windows, axis=1)])
        dd[: len(windows)] = np.expm1(np.min(path - np.maximum.accumulate(path, axis=1), axis=1))
    return pd.DataFrame({"return": forward, "risk": vol, "drawdown": dd}, index=returns.index)


def _pearson(x: Array, y: Array) -> float | None:
    if len(x) < 2:
        return None
    a, b = x - x.mean(), y - y.mean()
    denom = float(np.sqrt(np.dot(a, a) * np.dot(b, b)))
    if denom == 0 or not math.isfinite(denom):
        return None
    return float(np.clip(np.dot(a, b) / denom, -1, 1))


def _rank_ic(x: pd.Series, y: pd.Series) -> float | None:
    pairs = pd.concat([x, y], axis=1).dropna()
    ranks = pairs.rank(method="average").to_numpy(dtype=float)
    return _pearson(ranks[:, 0], ranks[:, 1])


def _bootstrap(x: Array, y: Array, h: int, rules: ScreenRules, ic: float) -> dict[str, Any]:
    rng = np.random.default_rng([rules.seed, h])
    collected = []
    for start in range(0, rules.bootstrap_draws, BOOTSTRAP_BATCH):
        draws = min(BOOTSTRAP_BATCH, rules.bootstrap_draws - start)
        indices = stationary_bootstrap_indices(len(x), draws, h, rng)
        a, b = x[indices], y[indices]
        a -= a.mean(axis=1, keepdims=True)
        b -= b.mean(axis=1, keepdims=True)
        with np.errstate(divide="ignore", invalid="ignore"):
            correlations = np.sum(a * b, axis=1) / np.sqrt(
                np.sum(a * a, axis=1) * np.sum(b * b, axis=1)
            )
        collected.append(np.clip(correlations, -1, 1))
    samples = np.concatenate(collected)
    # Degenerate bootstrap draws cannot support an interval or a p-value.
    if not np.isfinite(samples).all():
        return {"ic_low": None, "ic_high": None, "p_value": None}
    tail = (1 - rules.confidence) / 2
    low, high = np.quantile(samples, [tail, 1 - tail])
    return {
        "ic_low": float(low),
        "ic_high": float(high),
        "p_value": float(
            (1 + np.count_nonzero(np.abs(samples - ic) >= abs(ic))) / (len(samples) + 1)
        ),
    }


def _cut_returns(returns: pd.Series, screen_end: date) -> pd.Series:
    cut = returns.loc[returns.index <= pd.Timestamp(screen_end)].copy()
    if not isinstance(cut.index, pd.DatetimeIndex) or cut.index.hasnans:
        raise ValueError("market dates must be a DatetimeIndex without NaT")
    if not cut.index.is_unique or not cut.index.is_monotonic_increasing:
        raise ValueError("market dates must be unique and increasing")
    values = cut.to_numpy(dtype=float)
    if np.isinf(values).any() or (values <= -1).any():
        raise ValueError("market returns must be finite or NaN and greater than -1")
    return cut.astype(float)


def market_statistics(
    signal: pd.Series,
    returns: pd.Series,
    *,
    eval_start: date,
    screen_end: date,
    settings: ScreenRules,
    expected_sign: int,
    target: Literal["return", "risk"],
    bootstrap: bool,
) -> dict[str, Any]:
    returns = _cut_returns(returns, screen_end)
    signal = signal.reindex(returns.index)
    percentiles = expanding_percentile(signal, settings.warmup_days)
    groups = np.minimum(settings.groups - 1, np.floor(settings.groups * percentiles))
    horizons: dict[str, Any] = {}
    for h in settings.horizons:
        forward = _forward_values(returns, h)
        keep = (
            (returns.index >= pd.Timestamp(eval_start)) & signal.notna() & forward[target].notna()
        )
        x, y = signal[keep], forward.loc[keep, target]
        days = int(keep.sum())
        quintiles = []
        for group in range(settings.groups):
            mask = keep & (groups == group)
            count = int(mask.sum())
            quintiles.append(
                {
                    "group": group + 1,
                    "days": count,
                    "fwd_return": float(forward.loc[mask, "return"].mean() * TRADING_DAYS / h)
                    if count
                    else None,
                    "fwd_vol": float(forward.loc[mask, "risk"].mean()) if count else None,
                    "drawdown_share": float(
                        (forward.loc[mask, "drawdown"] <= settings.drawdown_threshold).mean()
                    )
                    if count
                    else None,
                }
            )
        ic = _rank_ic(x, y) if days >= settings.min_days else None
        group_values = [g["fwd_return" if target == "return" else "fwd_vol"] for g in quintiles]
        monotonicity = None
        if days >= settings.min_days and all(v is not None for v in group_values):
            monotonicity = _rank_ic(pd.Series(range(settings.groups)), pd.Series(group_values))
        item = {
            "horizon": h,
            "days": days,
            "n_eff": days / h,
            "mde": minimum_detectable_ic(days / h) if days >= settings.min_days else None,
            "ic": ic,
            "ic_low": None,
            "ic_high": None,
            "p_value": None,
            "quintiles": quintiles,
            "monotonicity": monotonicity,
        }
        if bootstrap and ic is not None:
            item.update(_bootstrap(x.rank().to_numpy(), y.rank().to_numpy(), h, settings, ic))
        horizons[str(h)] = item
    return cast(dict[str, Any], _json_plain({"horizons": horizons}))


def _checked_signal(
    function: SignalFunction,
    returns: pd.Series,
    sources: Mapping[str, pd.DataFrame],
    params: Mapping[str, Any],
) -> pd.Series:
    result = function(returns.copy(), {k: v.copy() for k, v in sources.items()}, dict(params))
    if not isinstance(result, pd.Series) or not result.index.equals(returns.index):
        raise ValueError("signal must return a Series on the input index")
    if not is_numeric_dtype(result.dtype) or is_complex_dtype(result.dtype):
        raise ValueError("signal must be numeric and real")
    if np.isinf(result.to_numpy(dtype=float)).any():
        raise ValueError("signal must not contain infinity")
    return result.astype(float)


def _strategy(
    loaded: LoadedSignal, returns: pd.Series, sources: Mapping[str, pd.DataFrame]
) -> tuple[checks.Strategy, pd.DataFrame]:
    # Synthetic column ids avoid collisions between returns, source ids and provider columns.
    inputs = pd.DataFrame({"returns": returns})
    columns: dict[str, list[tuple[Any, str]]] = {}
    for source, frame in sources.items():
        columns[source] = []
        for column in frame.columns:
            key = f"source_{len(inputs.columns)}"
            inputs[key] = frame[column]
            columns[source].append((column, key))

    def strategy(frame: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
        tables = {
            source: pd.DataFrame({col: frame[key] for col, key in names}, index=frame.index)
            for source, names in columns.items()
        }
        return _checked_signal(loaded.function, frame["returns"], tables, params)

    return strategy, inputs


def _leave_out(x: pd.Series, y: pd.Series, unit: str, expected_sign: int) -> dict[str, Any]:
    years = pd.DatetimeIndex(x.index).year
    units = years if unit == "year" else years // 10 * 10
    values = [_rank_ic(x[units != u], y[units != u]) for u in sorted(set(units))]
    finite = [v for v in values if v is not None]
    return {
        "unit": unit,
        "count": len(values),
        "min_ic": min(finite) if finite else None,
        "max_ic": max(finite) if finite else None,
        "stable": all(v is not None and v * expected_sign > 0 for v in values) if values else None,
    }


def _partial_ic(x: pd.Series, y: pd.Series, bases: Mapping[str, pd.Series]) -> float | None:
    data = pd.concat([x, y, *bases.values()], axis=1).dropna().rank()
    if len(data) < 2:
        return None
    matrix = data.to_numpy()
    design = np.column_stack([np.ones(len(matrix)), matrix[:, 2:]])
    residual = matrix[:, :2] - design @ np.linalg.lstsq(design, matrix[:, :2], rcond=None)[0]
    if (residual.std(axis=0) < RESIDUAL_EPS).any():
        return None
    return _pearson(residual[:, 0], residual[:, 1])


def screen_signal(
    loaded: LoadedSignal,
    markets: Mapping[str, ScreenMarket],
    sources: Mapping[str, pd.DataFrame],
    source_lags: Mapping[str, int | SourceSpec],
    settings: ScreenRules,
) -> dict[str, Any]:
    only = loaded.spec.markets == "qqq"
    required = {"qqq"} if only else {"qqq", settings.primary_market}
    problems = [f"missing market {k}" for k in sorted(required - markets.keys())]
    problems += [
        f"missing source or lag {k}"
        for k in loaded.spec.sources
        if k not in sources or k not in source_lags
    ]
    if problems:
        raise ScreenCheckError(problems)
    results: dict[str, Any] = {}
    unchecked: list[str] = []
    try:
        for market_id in sorted(markets if not only else {"qqq": markets["qqq"]}):
            market = markets[market_id]
            returns = _cut_returns(market.returns, settings.screen_end)
            aligned = {}
            for name in loaded.spec.sources:
                table = sources[name].loc[sources[name].index <= pd.Timestamp(settings.screen_end)]
                aligned[name] = pd.DataFrame(
                    {
                        col: align_source(
                            table[col], pd.DatetimeIndex(returns.index), source_lags[name]
                        )
                        for col in table.columns
                    },
                    index=returns.index,
                )
            strategy, inputs = _strategy(loaded, returns, aligned)
            with np.errstate(all="ignore"):
                signal = strategy(inputs, loaded.spec.params)
                if market_id in settings.bootstrap_markets:
                    cuts = checks.cut_dates(
                        pd.DatetimeIndex(returns.index),
                        pd.Timestamp(market.eval_start),
                        pd.Timestamp(settings.screen_end),
                        settings.lookahead_cut_dates,
                    )
                    findings = checks.check_no_lookahead(strategy, inputs, loaded.spec.params, cuts)
                    if findings:
                        raise ScreenCheckError([f"{market_id}: {p}" for p in findings])
                if market_id == ("qqq" if only else settings.primary_market):
                    findings, unchecked = checks.check_parameters_consumed(
                        strategy,
                        inputs,
                        loaded.spec.params,
                        pd.Timestamp(market.eval_start),
                        pd.Timestamp(settings.screen_end),
                    )
                    if findings:
                        raise ScreenCheckError(findings)
            stats = market_statistics(
                signal,
                returns,
                eval_start=market.eval_start,
                screen_end=settings.screen_end,
                settings=settings,
                expected_sign=loaded.spec.expected_sign,
                target=loaded.spec.target,
                bootstrap=market_id in settings.bootstrap_markets,
            )
            h = settings.primary_horizon
            forward = _forward_values(returns, h)[loaded.spec.target]
            keep = (
                (returns.index >= pd.Timestamp(market.eval_start))
                & signal.notna()
                & forward.notna()
            )
            x, y = signal[keep], forward[keep]
            bases = {k: v[keep] for k, v in baseline_signals(returns).items()}
            ic = stats["horizons"][str(h)]["ic"]
            stats.update(
                {
                    "eval_start": market.eval_start.isoformat(),
                    "first_day": x.index[0].date().isoformat() if len(x) else None,
                    "last_day": x.index[-1].date().isoformat() if len(x) else None,
                    "direction_matches": bool(ic * loaded.spec.expected_sign > 0)
                    if ic is not None
                    else None,
                    "leave_out": _leave_out(
                        x, y, "year" if market_id == "qqq" else "decade", loaded.spec.expected_sign
                    )
                    if len(x) >= settings.min_days
                    else {
                        "unit": "year" if market_id == "qqq" else "decade",
                        "count": 0,
                        "min_ic": None,
                        "max_ic": None,
                        "stable": None,
                    },
                    "baseline_corr": {k: _rank_ic(x, v) for k, v in bases.items()},
                    "partial_ic": _partial_ic(x, y, bases)
                    if market_id in settings.bootstrap_markets and len(x) >= settings.min_days
                    else None,
                }
            )
            results[market_id] = stats
    except ScreenCheckError:
        raise
    except Exception as exc:
        raise ScreenCheckError([f"signal evaluation: {exc}"]) from exc
    directions = [
        v["direction_matches"] for v in results.values() if v["direction_matches"] is not None
    ]
    breadth = sum(directions) / len(directions) if directions and not only else None
    correlations = [
        abs(v) for k in required for v in results[k]["baseline_corr"].values() if v is not None
    ]
    max_corr = max(correlations) if correlations else None
    qqq = results["qqq"]["horizons"][str(settings.primary_horizon)]
    primary = (
        results[settings.primary_market]["horizons"][str(settings.primary_horizon)]
        if not only
        else None
    )

    def interval(item: dict[str, Any] | None) -> list[float] | None:
        return (
            [item["ic_low"], item["ic_high"]]
            if item is not None and item["ic_low"] is not None and item["ic_high"] is not None
            else None
        )

    inputs_verdict = {
        "expected_sign": loaded.spec.expected_sign,
        "qqq_ic": qqq["ic"],
        "qqq_interval": interval(qqq),
        "primary_interval": interval(primary),
        "breadth": breadth,
        "leave_out_stable": results[settings.primary_market]["leave_out"]["stable"]
        if not only
        else None,
        "baseline_max_abs_corr": max_corr,
        "qqq_only": only,
    }
    return cast(
        dict[str, Any],
        _json_plain(
            {
                "markets": results,
                "unchecked": unchecked,
                "breadth": breadth,
                "direction_matches": int(sum(directions)),
                "direction_tested": len(directions),
                "baseline_max_abs_corr": max_corr,
                "primary_p_value": primary["p_value"] if primary else None,
                "verdict_inputs": inputs_verdict,
            }
        ),
    )


def screen_verdict(
    *,
    expected_sign: int,
    qqq_ic: float | None,
    qqq_interval: Sequence[float] | None,
    primary_interval: Sequence[float] | None,
    breadth: float | None,
    primary_adjusted_p: float | None,
    leave_out_stable: bool | None,
    baseline_max_abs_corr: float | None,
    qqq_only: bool,
    settings: ScreenRules,
) -> Literal["candidate", "eliminated", "inconclusive"]:
    interval = qqq_interval if qqq_only else primary_interval
    wrong = interval is not None and (interval[1] < 0 if expected_sign == 1 else interval[0] > 0)
    if (
        wrong
        or (breadth is not None and breadth < settings.eliminate_breadth)
        or (baseline_max_abs_corr is not None and baseline_max_abs_corr >= settings.copy_corr)
    ):
        return "eliminated"
    if (
        not qqq_only
        and qqq_ic is not None
        and qqq_ic * expected_sign > 0
        and breadth is not None
        and breadth >= settings.candidate_breadth - BREADTH_TOLERANCE
        and primary_adjusted_p is not None
        and primary_adjusted_p <= settings.bh_q
        and leave_out_stable is True
    ):
        return "candidate"
    return "inconclusive"


def _json_plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _json_plain(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_json_plain(v) for v in value]
    if isinstance(value, np.generic):
        return _json_plain(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value
