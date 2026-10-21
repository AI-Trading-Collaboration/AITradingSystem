"""One evaluation: checks, simulation, fixed benchmarks, metrics and a reproducible fingerprint.

The benchmarks are fixed and cannot be added after a run: the constant exposure equal to the
strategy's average exposure (the primary comparison), buy-and-hold QQQ, cash and, for questions
that use leverage, fixed 2x and 3x. The development window is open; the validation and consistency
windows open only with a permit that ``research_bench.ledger`` issues when it registers a read.
Research runs go through the ledger (``run_and_record``), which records every run. On request
a run also faces the standard falsification (``research_bench.falsify``); the ledger asks for it on
every validation and consistency run.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd

from ai_trading_system.research_bench.checks import (
    Strategy,
    check_no_lookahead,
    check_parameters_consumed,
    cut_dates,
    strategy_targets,
)
from ai_trading_system.research_bench.data import BenchData, strategy_returns
from ai_trading_system.research_bench.events import derisk_events
from ai_trading_system.research_bench.falsify import FalsificationInputs, falsify
from ai_trading_system.research_bench.metrics import compare, exposure_gap, summarize
from ai_trading_system.research_bench.portfolio import portfolio_section
from ai_trading_system.research_bench.screen_settings import (
    LoadedScreenSettings,
    load_screen_settings,
)
from ai_trading_system.research_bench.segments import segment_section
from ai_trading_system.research_bench.settings import (
    EpisodeSpec,
    LoadedSettings,
    Window,
    WindowRole,
    load_settings,
    resolve_window,
)
from ai_trading_system.research_bench.simulate import (
    Rebalance,
    Simulation,
    constant_targets,
    simulate,
)

OPEN_WINDOW_ROLES: tuple[WindowRole, ...] = ("development",)
PRIMARY_BENCHMARK = "matched_constant"
MATCHED_VOLATILITY = "matched_volatility"
FINGERPRINT_DIGITS = 12


class BenchCheckError(ValueError):
    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


class WindowNotOpenError(PermissionError):
    """The window cannot be read without a registered read."""


@dataclass(frozen=True)
class WindowPermit:
    """A registered read of a validation or consistency window (issued by the ledger only)."""

    question_id: str
    role: WindowRole
    read_id: str
    strategies: tuple[str, ...]


@dataclass(frozen=True)
class RunSpec:
    question_type: str
    strategy_id: str
    params: Mapping[str, Any]
    window_role: WindowRole
    rebalance: Rebalance = field(default_factory=Rebalance)
    delay_days: int = 0
    cost_bps: float | None = None
    leveraged_benchmarks: bool = False
    reserve: str = "cash"


@dataclass(frozen=True)
class RunResult:
    spec: dict[str, Any]
    window: dict[str, Any]
    source: dict[str, Any]
    settings_sha256: str
    cost_bps: float
    checks: dict[str, Any]
    strategy: dict[str, Any]
    benchmarks: dict[str, dict[str, Any]]
    comparison: dict[str, Any]
    exposure_gap: dict[str, float]
    fingerprint: str
    falsification: dict[str, Any] | None = None
    events: dict[str, Any] | None = None
    diagnostics: dict[str, Any] | None = None
    segments: dict[str, Any] | None = None
    portfolio: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def open_window(
    loaded: LoadedSettings,
    question_type: str,
    role: WindowRole,
    permit: WindowPermit | None = None,
) -> Window:
    window = resolve_window(loaded, question_type, role)
    if role in OPEN_WINDOW_ROLES:
        return window
    if permit is None or permit.role != role:
        raise WindowNotOpenError(
            f"the {role} window opens only through a read registered by research_bench.ledger"
        )
    return window


def run_strategy(
    data: BenchData,
    strategy: Strategy,
    spec: RunSpec,
    loaded: LoadedSettings | None = None,
    *,
    permit: WindowPermit | None = None,
    falsification_confidence: float | None = None,
    screen: LoadedScreenSettings | None = None,
    episodes: Sequence[EpisodeSpec] | None = None,
    slow_bears: Sequence[EpisodeSpec] = (),
    long_bulls: Sequence[EpisodeSpec] = (),
    declines: Sequence[EpisodeSpec] = (),
    portfolio_weight: float | None = None,
) -> RunResult:
    """Evaluate one strategy; with ``falsification_confidence`` also run the standard
    falsification at that confidence (it is not part of the fingerprint).
    ``episodes`` 是减仓事件表的固定下跌段，不给时用评估台设置里的。
    ``slow_bears``、``long_bulls`` 是只读段表的固定段（``research_bench.segments``），
    不进指纹、不进规则，两样都不给时结果的 ``segments`` 为 None。
    给了 ``portfolio_weight``（固定比例配置的 QQQ 权重）时储备不能是现金，``declines``
    是窗口的 10% 下跌段，结果加 ``portfolio`` 一节、不进指纹；不给时为 None。
    """

    if portfolio_weight is not None:
        if spec.reserve == "cash":
            raise ValueError("the portfolio ruler needs a reserve other than cash")
        if not declines:
            raise ValueError("the portfolio ruler needs the window's declines")
    loaded = loaded or load_settings()
    window = open_window(loaded, spec.question_type, spec.window_role, permit)
    params = _json_params(spec.params)
    start, end = _window_bounds(data, window)

    targets = strategy_targets(strategy, data.inputs, params)
    problems = check_no_lookahead(
        strategy,
        data.inputs,
        params,
        cut_dates(
            pd.DatetimeIndex(data.inputs.index), start, end, loaded.settings.lookahead_cut_dates
        ),
    )
    consumed, unchecked = check_parameters_consumed(strategy, data.inputs, params, start, end)
    problems.extend(consumed)
    if problems:
        raise BenchCheckError(problems)

    traded = strategy_returns(data, spec.reserve)
    if spec.reserve != "cash":
        missing = traded.loc[(traded.index > start) & (traded.index <= end), "cash"].isna()
        if missing.any():
            when = missing.index[missing][0].date()
            raise ValueError(f"reserve {spec.reserve} has no return on {when} inside the window")
    cost_bps = loaded.cost_bps if spec.cost_bps is None else float(spec.cost_bps)
    sim = simulate(
        traded,
        targets,
        start=window.start,
        end=window.end,
        rebalance=spec.rebalance,
        cost_bps=cost_bps,
        delay_days=spec.delay_days,
    )
    benchmark_sims = benchmark_simulations(
        data,
        window,
        average_exposure=float(sim.exposure.mean()),
        rebalance=spec.rebalance,
        cost_bps=cost_bps,
        leveraged=spec.leveraged_benchmarks,
    )
    cash = data.returns["cash"]
    strategy_summary = summarize(sim, cash, loaded)
    benchmarks = {name: summarize(item, cash, loaded) for name, item in benchmark_sims.items()}
    comparison = compare(strategy_summary, benchmarks[PRIMARY_BENCHMARK], loaded)
    gap = exposure_gap(sim, float(sim.exposure.mean()), data.returns["qqq"])
    screen = screen or load_screen_settings()
    events = derisk_events(
        sim.exposure,
        data.returns["qqq"].reindex(sim.exposure.index),
        reference_exposure=float(sim.exposure.mean()),
        settings=screen.settings.derisk_events,
        episodes=loaded.settings.episodes if episodes is None else tuple(episodes),
        trading_days_per_year=loaded.settings.trading_days_per_year,
    )
    falsification = None
    if falsification_confidence is not None:
        falsification = falsify(
            FalsificationInputs(
                returns=data.returns,
                targets=targets,
                start=window.start,
                end=window.end,
                rebalance=spec.rebalance,
                delay_days=spec.delay_days,
                cost_bps=cost_bps,
                strategy=sim,
                benchmark=benchmark_sims[PRIMARY_BENCHMARK],
                strategy_returns=None if spec.reserve == "cash" else traded,
            ),
            loaded,
            falsification_confidence,
        )

    spec_payload = spec_record(spec)
    window_record = {
        "window_id": window.window_id,
        "role": window.role,
        "start": window.start.isoformat(),
        "end": None if window.end is None else window.end.isoformat(),
    }
    checks = {
        "lookahead_cut_dates": loaded.settings.lookahead_cut_dates,
        "parameters_unchecked": unchecked,
        "problems": [],
    }
    fingerprint = _fingerprint(
        {
            "spec": spec_payload,
            "window": window_record,
            "source": data.source,
            "settings_sha256": loaded.sha256,
            "cost_bps": cost_bps,
            "strategy": strategy_summary,
            "benchmarks": benchmarks,
        }
    )
    return RunResult(
        spec=spec_payload,
        window=window_record,
        source=dict(data.source),
        settings_sha256=loaded.sha256,
        cost_bps=cost_bps,
        checks=checks,
        strategy=strategy_summary,
        benchmarks=benchmarks,
        comparison=comparison,
        exposure_gap=gap,
        fingerprint=fingerprint,
        falsification=falsification,
        events=events,
        diagnostics={
            MATCHED_VOLATILITY: matched_volatility(
                data,
                window,
                strategy_summary,
                benchmarks["buy_and_hold_qqq"],
                rebalance=spec.rebalance,
                cost_bps=cost_bps,
                loaded=loaded,
            )
        },
        segments=segment_section(
            sim, benchmark_sims[PRIMARY_BENCHMARK], tuple(slow_bears), tuple(long_bulls)
        ),
        portfolio=None
        if portfolio_weight is None
        else portfolio_section(
            sim,
            benchmark_sims[PRIMARY_BENCHMARK],
            strategy_summary,
            benchmarks[PRIMARY_BENCHMARK],
            data.returns,
            traded["cash"],
            float(portfolio_weight),
            tuple(declines),
            loaded.settings.trading_days_per_year,
        ),
    )


def matched_volatility(
    data: BenchData,
    window: Window,
    strategy: dict[str, Any],
    qqq: dict[str, Any],
    *,
    rebalance: Rebalance,
    cost_bps: float,
    loaded: LoadedSettings,
) -> dict[str, Any] | None:
    """Descriptive QQQ/cash control, not a benchmark or part of the fingerprint."""

    denominator = float(qqq["annual_volatility"])
    ratio = float(strategy["annual_volatility"]) / denominator if denominator != 0 else math.nan
    if not math.isfinite(ratio):
        return None
    exposure = min(1.0, max(0.0, ratio))
    sim = simulate(
        data.returns,
        constant_targets(data.returns.index, exposure),
        start=window.start,
        end=window.end,
        rebalance=rebalance,
        cost_bps=cost_bps,
    )
    summary = summarize(sim, data.returns["cash"], loaded)
    return {
        "exposure": exposure,
        "summary": summary,
        "comparison": compare(strategy, summary, loaded),
    }


def spec_record(spec: RunSpec) -> dict[str, Any]:
    """The run specification as plain JSON (the cost is recorded separately, resolved)."""

    payload = {
        "question_type": spec.question_type,
        "strategy_id": spec.strategy_id,
        "params": _json_params(spec.params),
        "window_role": spec.window_role,
        "rebalance": {"schedule": spec.rebalance.schedule, "band": spec.rebalance.band},
        "delay_days": spec.delay_days,
        "leveraged_benchmarks": spec.leveraged_benchmarks,
    }
    if spec.reserve != "cash":
        payload["reserve"] = spec.reserve
    return payload


def benchmark_simulations(
    data: BenchData,
    window: Window,
    *,
    average_exposure: float,
    rebalance: Rebalance,
    cost_bps: float,
    leveraged: bool,
) -> dict[str, Simulation]:
    index = data.returns.index
    plans: dict[str, tuple[float, Rebalance]] = {
        PRIMARY_BENCHMARK: (average_exposure, rebalance),
        "buy_and_hold_qqq": (1.0, Rebalance()),
        "cash": (0.0, Rebalance()),
    }
    if leveraged:
        plans["fixed_2x"] = (2.0, Rebalance())
        plans["fixed_3x"] = (3.0, Rebalance())
    return {
        name: simulate(
            data.returns,
            constant_targets(index, exposure),
            start=window.start,
            end=window.end,
            rebalance=plan,
            cost_bps=cost_bps,
        )
        for name, (exposure, plan) in plans.items()
    }


def benchmark_summaries(
    data: BenchData,
    loaded: LoadedSettings,
    *,
    question_type: str = "qqq_direction",
    role: WindowRole = "development",
) -> tuple[Window, dict[str, dict[str, Any]]]:
    """The fixed benchmarks alone on an open window: a check of the data and the bench, not a
    strategy trial."""

    window = open_window(loaded, question_type, role)
    sims = benchmark_simulations(
        data,
        window,
        average_exposure=1.0,
        rebalance=Rebalance(),
        cost_bps=loaded.cost_bps,
        leveraged=True,
    )
    sims.pop(PRIMARY_BENCHMARK)
    cash = data.returns["cash"]
    return window, {name: summarize(sim, cash, loaded) for name, sim in sims.items()}


def _window_bounds(data: BenchData, window: Window) -> tuple[pd.Timestamp, pd.Timestamp]:
    index = pd.DatetimeIndex(data.inputs.index)
    start = pd.Timestamp(window.start)
    end = index[-1] if window.end is None else pd.Timestamp(window.end)
    return start, end


def _json_params(params: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return dict(json.loads(json.dumps(dict(params), sort_keys=True)))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"strategy parameters must be plain JSON values: {exc}") from None


def _fingerprint(payload: Mapping[str, Any]) -> str:
    text = json.dumps(_rounded(payload), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _rounded(value: Any) -> Any:
    if isinstance(value, float):
        if not math.isfinite(value):
            return repr(value)
        return float(f"{value:.{FINGERPRINT_DIGITS}g}")
    if isinstance(value, Mapping):
        return {str(key): _rounded(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_rounded(item) for item in value]
    return value
