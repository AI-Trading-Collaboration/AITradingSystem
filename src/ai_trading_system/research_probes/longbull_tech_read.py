"""在科技行业上直接读一次 F0001 的长牛代价（TRADING-2565-70）。

依据 owner_decision:TRADING-2565:2026-10-26:longbull_tech_read、第 6 期章程第 17 节。
F0001 是 QQQ-Q3 的 fast_slow。行业收益减 RF 拼成超额收益和 RF，交给评估台自己的
from_us_market；目标、模拟、固定对照都用评估台的函数，评估台不改。
划段和读数沿用 cycle_power、longbull_power，两个模块不改。
先核美股大盘计算路径和四条序列的长牛段；任何一条不过或出错，两个变体都判
incomplete，科技行业一步都不算。读时任何错误都丢弃全部行业读数，错误信息去掉小数。
tech_ew 用段间 t 0.99，three 合并三个行业的段用 0.997；上界 < 0 为 loss，
下界 > 0 为 gain，否则 undecided；段数不足为 incomplete。主读数 three，
下界 > −1.67 对数点只印，不参与判定。只读一次、不进任何账本、不算试验、k 不动，
只读到 2010-12-31；读后错误不得借本模块静默重读。
TRADING-2565-71 仅改美股核对：首次 run 停因两位小数年化差来自 -34 一位小数段内收益手算。
现逐段对 -34 印的收益、平均敞口及 JSON 全精度三数：长牛年化差均值、最差、整段平均敞口。
TRADING-2565-72：第二次 run 停在建 Softw 的数据，因为序列中间缺一天；现在每条序列
去掉全部缺值日，F0001 和划段（cut_of）、cycle_power 用同一条序列，去掉的日子在划段核对里印出。
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import segments
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import BenchData, from_us_market
from ai_trading_system.research_bench.prereg import StrategyEntry
from ai_trading_system.research_bench.run import PRIMARY_BENCHMARK, benchmark_simulations
from ai_trading_system.research_bench.settings import (
    LoadedSettings,
    Window,
    load_settings,
    resolve_window,
)
from ai_trading_system.research_bench.signal_strategy import signal_exposure
from ai_trading_system.research_bench.simulate import (
    Simulation,
    constant_targets,
    schedule_mask,
    simulate,
)
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_probes import backfill_check
from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import longbull_power as lp
from ai_trading_system.research_probes import longbull_realpath as rr

# 第 6 期章程第 17 节：跑前固定的变体、尺子和判法，不重新选尺子。
RUN_ID = "TRADING-2565-70"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-26:longbull_tech_read"
MARKET = lp.MARKET
SERIES = (cp.TECH, *cp.COLUMNS)
VARIANTS = cp.VARIANTS
PRIMARY = "three"
CONFIDENCE = {"tech_ew": 0.99, "three": 0.997}
PRINT_LINE = lp.PRINT_LINE
# TRADING-2565-63 汇报印的埋规则读数的标准差（500 条历史上点估计的标准差），只印比值
PLANT_SE = {"tech_ew": 0.0192, "three": 0.0149}
US_LONG_BULLS = rr.LONG_BULLS
# TRADING-2565-34 印的真实 F0001 在美股大盘 1929–2010 七个长牛段的段内收益、
# 同平均敞口固定对照的段内收益、F0001 段内平均敞口（一位小数的百分数）
US_PRINTED = (
    (1.678, 1.469, 0.691),
    (1.170, 1.186, 0.793),
    (3.749, 3.737, 0.752),
    (0.958, 1.047, 0.737),
    (4.801, 5.747, 0.721),
    (4.338, 4.284, 0.773),
    (0.776, 0.742, 0.753),
)
US_REFERENCE = "TRADING-2565-34 印的七段段内收益和段内平均敞口（TRADING-2565-71）"
# TRADING-2565-34 的 JSON（fast_slow、us_1929、stage1）印的全精度数，线是 TOLERANCE
US_ANCHORS = (
    ("mean_annual_difference", 0.0010994206923715666),
    ("worst_annual_difference", -0.01347890310397748),
    ("average_exposure", 0.6557715110992798),
)
ANCHOR_TEXT = {
    "mean_annual_difference": "长牛年化差的平均",
    "worst_annual_difference": "长牛年化差的最差",
    "average_exposure": "整段平均敞口",
}
# 从上面的段内收益按 年数 = 月数 / 12 手算的简单年化差（点），只印
US_HAND = (0.0257, -0.0022, 0.0002, -0.0077, -0.0135, 0.0009, 0.0042)
US_SD = lp.TARGET_SD
# 一位小数百分数的半格；标准差的线一格（规格第 3 节第 3 步）
PRINTED_STEP, SD_STEP = 0.0005, 0.0001
# TRADING-2565-63 汇报印的四条科技序列的真实长牛段
TECH_LONG_BULLS = {
    "tech_ew": tuple(
        f"long_bull_{m}"
        for m in ("194007", "194706", "196211", "197501", "198408", "199011", "200210")
    ),
    "Hardw": tuple(
        f"long_bull_{m}"
        for m in ("194007", "194705", "196207", "197410", "198110", "199308", "200210")
    ),
    "Chips": tuple(
        f"long_bull_{m}"
        for m in ("194201", "194803", "196211", "197410", "198408", "199011", "200409")
    ),
    "Softw": ("long_bull_199010", "long_bull_200210"),
}
# 和评估台逐日对的容限（TRADING-2565-56 用的），不是研究阈值
TOLERANCE = 1e-12
# 只消除浮点比较的舍入误差，不改变核对的线
SLACK = 1e-12
SUMMARY, REPORT = "tech_read.json", "tech_read.md"
# 错误信息里带小数点的数一律换掉，科技行业上的数不经错误信息出来（规格第 3 节）
NUMBER = re.compile(r"[-+]?\d*\.\d+(?:[eE][-+]?\d+)?")
VERDICTS = ("loss", "undecided", "gain", "incomplete")

Array = np.ndarray[Any, Any]
Record = dict[str, Any]
Progress = Callable[[str], None]

app = typer.Typer()


@dataclass(frozen=True)
class Inputs:
    industries: pd.DataFrame
    factors: pd.DataFrame
    bench_us: BenchData
    window: Window
    entry: StrategyEntry
    loaded: LoadedSettings


@dataclass(frozen=True)
class Pair:
    strategy: Simulation
    benchmark: Simulation


@dataclass(frozen=True)
class Cut:
    level: pd.Series
    bulls: list[segments.Segment]
    bears: list[segments.Segment]
    spans: list[tuple[int, int]]


def raw_series(industries: pd.DataFrame) -> dict[str, pd.Series]:
    industries = cp.cut(industries)
    missing = [name for name in cp.COLUMNS if name not in industries]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    values = {cp.TECH: industries.loc[:, list(cp.COLUMNS)].mean(axis=1)}
    values.update({name: industries[name] for name in cp.COLUMNS})
    result = {}
    for name, daily in values.items():
        first = daily.first_valid_index()
        if first is None:
            raise ValueError(f"{name} has no returns")
        result[name] = daily.loc[first:].astype(float)
    return result


def series_returns(industries: pd.DataFrame, factors: pd.DataFrame) -> dict[str, pd.Series]:
    return {name: daily.dropna() for name, daily in raw_series(industries).items()}


def missing_days(industries: pd.DataFrame) -> dict[str, list[str]]:
    return {
        name: [
            pd.Timestamp(day).date().isoformat() for day in daily.index[daily.isna()].sort_values()
        ]
        for name, daily in raw_series(industries).items()
    }


def bench_data(returns: pd.Series, cash: pd.Series) -> BenchData:
    rf = cash.reindex(returns.index)
    if rf.isna().any():
        when = pd.Timestamp(rf.index[rf.isna()][0]).date()
        raise ValueError(f"missing RF on {when}")
    frame = pd.DataFrame({MARKET: returns - rf, cp.CASH: rf})
    return from_us_market({cp.FACTORS: frame}, snapshot_id=cp.SNAPSHOT_ID)


def f0001_targets(data: BenchData, entry: StrategyEntry) -> pd.Series:
    return strategy_targets(signal_exposure, data.inputs, entry.params)


def window_start(data: BenchData, entry: StrategyEntry, first_day: date = cp.FIRST_DAY) -> date:
    index = pd.DatetimeIndex(data.returns.index)
    targets = f0001_targets(data, entry).to_numpy(dtype=float)
    executable = np.flatnonzero(
        np.asarray(schedule_mask(index, entry.rebalance.schedule)) & np.isfinite(targets)
    )
    if not len(executable) or executable[0] + 1 >= len(index):
        raise ValueError("F0001 has no executable target")
    return max(first_day, index[int(executable[0]) + 1].date())


def f0001_pair(
    data: BenchData,
    entry: StrategyEntry,
    loaded: LoadedSettings,
    start: date,
    end: date = cp.LAST_DAY,
) -> Pair:
    rebalance = entry.rebalance.to_rebalance()
    strategy = simulate(
        data.returns,
        f0001_targets(data, entry),
        start=start,
        end=end,
        rebalance=rebalance,
        cost_bps=loaded.cost_bps,
    )
    benchmark = simulate(
        data.returns,
        constant_targets(data.returns.index, float(strategy.exposure.mean())),
        start=start,
        end=end,
        rebalance=rebalance,
        cost_bps=loaded.cost_bps,
    )
    return Pair(strategy, benchmark)


def bench_pair(
    data: BenchData, entry: StrategyEntry, loaded: LoadedSettings, window: Window
) -> Pair:
    rebalance = entry.rebalance.to_rebalance()
    strategy = simulate(
        data.returns,
        f0001_targets(data, entry),
        start=window.start,
        end=window.end if window.end is not None else cp.LAST_DAY,
        rebalance=rebalance,
        cost_bps=loaded.cost_bps,
    )
    benchmark = benchmark_simulations(
        data,
        window,
        average_exposure=float(strategy.exposure.mean()),
        rebalance=rebalance,
        cost_bps=loaded.cost_bps,
        leveraged=False,
    )[PRIMARY_BENCHMARK]
    return Pair(strategy, benchmark)


def cut_of(daily: pd.Series) -> Cut:
    level = segments.monthly_level(daily.dropna())
    drops = segments.drop_segments(level)
    bulls = segments.long_bull_segments(level, drops)
    spans = lp.long_bulls(level)
    if len(bulls) != len(spans):
        raise ValueError("long bulls differ between segments and longbull_power")
    return Cut(level, bulls, [item for item in drops if item.kind == "slow_bear"], spans)


def month_offsets(index: pd.Index, level: pd.Series) -> Array:
    months = pd.DatetimeIndex(index).to_period("M").astype("int64").to_numpy(dtype=np.int64)
    return np.asarray(months - pd.Period(level.index[0], freq="M").ordinal, dtype=np.int64)


def readings_of(pair: Pair, cut: Cut) -> Array:
    own, reference = pair.strategy.returns, pair.benchmark.returns
    if not own.index.equals(reference.index):
        raise ValueError("strategy and benchmark have different dates")
    month = month_offsets(own.index, cut.level)
    for item, (first, _) in zip(cut.bulls, cut.spans, strict=True):
        if first <= month[0]:
            raise ValueError(f"{item.id} starts before F0001's first full month")
    difference = np.log1p(own.to_numpy(dtype=float)) - np.log1p(reference.to_numpy(dtype=float))
    return lp.readings(difference, month, cut.spans)


def bull_rows(pair: Pair, cut: Cut, values: Array) -> list[Record]:
    rows = segments.segment_rows(pair.strategy, pair.benchmark, cut.bulls)
    whole = float(pair.strategy.exposure.mean())
    return [
        {
            "id": item.id,
            "start": item.start,
            "end": item.end,
            "months": item.months,
            "reading": float(value),
            "simple": row["annual_difference"],
            "exposure": row["average_exposure"],
            "whole": whole,
        }
        for item, value, row in zip(cut.bulls, values, rows, strict=True)
    ]


def bear_rows(pair: Pair, cut: Cut) -> tuple[list[Record], list[str]]:
    first = pd.Period(pair.strategy.returns.index[0], freq="M")
    covered = [item for item in cut.bears if pd.Period(item.start, freq="M") > first]
    uncovered = [item.id for item in cut.bears if pd.Period(item.start, freq="M") <= first]
    rows = segments.segment_rows(pair.strategy, pair.benchmark, covered)
    return [
        {
            "id": item.id,
            "start": item.start,
            "end": item.end,
            "strategy": row["strategy_return"],
            "benchmark": row["benchmark_return"],
            "difference": row["difference"],
            "exposure": row["average_exposure"],
        }
        for item, row in zip(covered, rows, strict=True)
    ], uncovered


def us_check(inputs: Inputs) -> Record:
    factors = cp.cut(inputs.factors)
    cut = cut_of(lp.us_frame(factors)[cp.TECH])
    found = [(item.start, item.end) for item in cut.bulls]
    bulls_match = found == list(US_LONG_BULLS)
    data = bench_data(factors[MARKET] + factors[cp.CASH], factors[cp.CASH])
    start = window_start(data, inputs.entry)
    own = f0001_pair(data, inputs.entry, inputs.loaded, start)
    bench = bench_pair(inputs.bench_us, inputs.entry, inputs.loaded, inputs.window)
    same_dates = (
        own.strategy.start == bench.strategy.start
        and own.benchmark.start == bench.benchmark.start
        and own.strategy.daily.index.equals(bench.strategy.daily.index)
        and own.benchmark.daily.index.equals(bench.benchmark.daily.index)
    )
    gaps = {
        key: float(np.max(np.abs(left.to_numpy(dtype=float) - right.to_numpy(dtype=float))))
        if same_dates
        else None
        for key, left, right in (
            ("exposure", own.strategy.exposure, bench.strategy.exposure),
            ("return", own.strategy.returns, bench.strategy.returns),
            ("benchmark", own.benchmark.returns, bench.benchmark.returns),
        )
    }
    rows = segments.segment_rows(own.strategy, own.benchmark, cut.bulls)
    simple = [float(row["annual_difference"]) for row in rows]
    totals = []
    for i, row in enumerate(rows):
        values = [
            float(row[key]) for key in ("strategy_return", "benchmark_return", "average_exposure")
        ]
        printed = list(US_PRINTED[i]) if i < len(US_PRINTED) else None
        totals.append(
            {
                "strategy": values[0],
                "benchmark": values[1],
                "exposure": values[2],
                "printed": printed,
                "match": printed is not None
                and all(
                    abs(value - reference) <= PRINTED_STEP + SLACK
                    for value, reference in zip(values, printed, strict=True)
                ),
            }
        )
    totals_match = len(totals) == len(US_PRINTED) and all(row["match"] for row in totals)
    exposure = float(own.strategy.exposure.mean())
    summary = segments.long_bull_summary(rows)
    anchors = []
    for name, printed_value in US_ANCHORS:
        value = exposure if name == "average_exposure" else summary[name]
        difference = value - printed_value if value is not None else None
        anchors.append(
            {
                "name": name,
                "value": value,
                "printed": printed_value,
                "difference": difference,
                "match": difference is not None and abs(difference) <= TOLERANCE,
            }
        )
    anchors_match = all(row["match"] for row in anchors)
    sd = lp.sample_sd(np.asarray(simple, dtype=float))
    sd_match = sd is not None and abs(sd - US_SD) <= SD_STEP + SLACK
    months = pd.DatetimeIndex(own.strategy.exposure.index).to_period("M")
    inside = np.zeros(len(months), dtype=bool)
    for first, last in found:
        inside |= (months >= pd.Period(first, "M")) & (months <= pd.Period(last, "M"))
    return {
        "long_bulls": [list(span) for span in found],
        "bulls_match": bulls_match,
        "start": own.strategy.start.date().isoformat(),
        "bench_start": bench.strategy.start.date().isoformat(),
        "same_dates": same_dates,
        "gaps": gaps,
        "totals": totals,
        "totals_match": totals_match,
        "anchors": anchors,
        "anchors_match": anchors_match,
        "simple": simple,
        "hand": list(US_HAND),
        "sd": sd,
        "sd_reference": US_SD,
        "sd_match": sd_match,
        "exposure": exposure,
        "bull_exposure": float(own.strategy.exposure[inside].mean()) if inside.any() else None,
        "printed_exposure": [rr.F0001_EXPOSURE, rr.F0001_BULL_EXPOSURE],
        "passed": bool(
            bulls_match
            and same_dates
            and totals_match
            and anchors_match
            and sd_match
            and all(value is not None and value <= TOLERANCE for value in gaps.values())
        ),
    }


def tech_check(industries: pd.DataFrame, factors: pd.DataFrame) -> tuple[dict[str, Cut], Record]:
    frame = cp.market_frame(industries, factors)
    cuts = {name: cut_of(frame[name]) for name in SERIES}
    dropped = missing_days(industries)
    rows = {}
    for name, cut in cuts.items():
        found = [item.id for item in cut.bulls]
        expected = list(TECH_LONG_BULLS[name])
        rows[name] = {
            "found": found,
            "expected": expected,
            "match": found == expected,
            "dropped": dropped[name],
        }
    return cuts, {"series": rows, "passed": all(row["match"] for row in rows.values())}


def incomplete_variant(variant: str) -> Record:
    return {
        "variant": variant,
        "confidence": CONFIDENCE[variant],
        "segments": 0,
        "mean": None,
        "low": None,
        "high": None,
        "verdict": "incomplete",
        "line": None,
        "se": None,
        "ratio": None,
        "simple_sd": None,
    }


def variant_summary(variant: str, values: Sequence[float], simple: Sequence[float]) -> Record:
    if len(values) < lp.MIN_SEGMENTS:
        return {**incomplete_variant(variant), "segments": len(values)}
    array = np.asarray(values, dtype=float)
    confidence = CONFIDENCE[variant]
    low, high = lp.t_bounds(array, (confidence,))[f"{confidence:g}"]
    se = float(np.std(array, ddof=1)) / math.sqrt(len(array))
    return {
        "variant": variant,
        "confidence": confidence,
        "segments": len(array),
        "mean": float(array.mean()),
        "low": low,
        "high": high,
        "verdict": cp.judge(low, high),
        "line": bool(low > PRINT_LINE),
        "se": se,
        "ratio": se / PLANT_SE[variant],
        "simple_sd": lp.sample_sd(np.asarray(simple, dtype=float)),
    }


def _error(name: str, exc: Exception) -> str:
    message = str(exc).splitlines()
    text = f"{name}: {type(exc).__name__}"
    if message and message[0]:
        text += f": {message[0]}"
    return NUMBER.sub("…", text)


def read_series(
    inputs: Inputs, cuts: Mapping[str, Cut], progress: Progress | None = None
) -> tuple[Record, list[str]]:
    errors: list[str] = []
    built: dict[str, BenchData] = {}
    try:
        returns = series_returns(inputs.industries, inputs.factors)
        cash = cp.cut(inputs.factors)[cp.CASH]
    except Exception as exc:
        return {}, [_error("series", exc)]
    for name in SERIES:
        try:
            built[name] = bench_data(returns[name], cash)
        except Exception as exc:
            errors.append(_error(name, exc))
    if errors:
        return {}, errors
    result: Record = {}
    for name in SERIES:
        try:
            data = built[name]
            start = window_start(data, inputs.entry)
            pair = f0001_pair(data, inputs.entry, inputs.loaded, start)
            values = readings_of(pair, cuts[name])
            bears, uncovered = bear_rows(pair, cuts[name])
            bulls = bull_rows(pair, cuts[name], values)
            result[name] = {
                "first_day": pd.Timestamp(data.returns.index[0]).date().isoformat(),
                "start": pair.strategy.start.date().isoformat(),
                "first_return": pd.Timestamp(pair.strategy.returns.index[0]).date().isoformat(),
                "days": len(pair.strategy.returns),
                "exposure": float(pair.strategy.exposure.mean()),
                "long_bulls": bulls,
                "slow_bears": bears,
                "uncovered": uncovered,
            }
        except Exception as exc:
            errors.append(_error(name, exc))
        if progress is not None:
            progress(name)
    return ({} if errors else result), errors


def read(inputs: Inputs, progress: Progress | None = None) -> Record:
    us: Record | None = None
    tech: Record | None = None
    errors: list[str] = []
    series: Record = {}
    stage = "checks"
    try:
        cuts, tech = tech_check(inputs.industries, inputs.factors)
        us = us_check(inputs)
    except Exception as exc:
        errors.append(_error("checks", exc))
    else:
        if tech["passed"] and us["passed"]:
            series, errors = read_series(inputs, cuts, progress=progress)
            stage = "series" if errors else "read"
    variants = {variant: incomplete_variant(variant) for variant in VARIANTS}
    if stage == "read":
        for variant in VARIANTS:
            names = (cp.TECH,) if variant == cp.TECH else cp.COLUMNS
            rows = [row for name in names for row in series[name]["long_bulls"]]
            variants[variant] = variant_summary(
                variant, [row["reading"] for row in rows], [row["simple"] for row in rows]
            )
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": cp.SNAPSHOT_ID,
        "parameters": {
            "first_day": cp.FIRST_DAY.isoformat(),
            "last_day": cp.LAST_DAY.isoformat(),
            "window": inputs.window.window_id,
            "config": backfill_check.CONFIGS[0].id,
            "rebalance": inputs.entry.rebalance.schedule,
            "cost_bps": inputs.loaded.cost_bps,
            "confidence": dict(CONFIDENCE),
            "primary": PRIMARY,
            "print_line": PRINT_LINE,
            "plant_se": dict(PLANT_SE),
            "tolerance": TOLERANCE,
            "us_reference": US_REFERENCE,
            "printed_step": PRINTED_STEP,
            "us_anchors": dict(US_ANCHORS),
            "sd_step": SD_STEP,
        },
        "stage": stage,
        "checks": {"us": us, "tech": tech},
        "errors": errors,
        "series": series,
        "variants": variants,
        "verdict": variants[PRIMARY]["verdict"],
    }


VERDICT_TEXT = {"loss": "有代价", "undecided": "不定", "gain": "有收益", "incomplete": "没判"}
ACTION_TEXT = {
    "loss": "F0001 在高增长行业的长牛里系统地少赚，QQQ 那段不是个别；"
    "11-20 续期卡问 F0001 在 QQQ 上要不要减重，或者换一个长牛里少吃亏的慢腿。",
    "undecided": "这份数据答不了；长牛代价问题在科技行业代理上到此为止，"
    "`research/families.md` 经典趋势一行记一句，长牛代价只剩前瞻账本 F0008 慢慢攒。",
    "gain": "F0001 在科技行业的长牛里还多赚，QQQ 2002–07 是个别；"
    "11-20 续期卡写「F0001 照留，长牛代价不是理由」。",
    "incomplete": "没读：核对没过或出错，科技行业上的 F0001 读数没有算；修什么先回循环线程。",
}


def _points(value: float | None, *, signed: bool = False) -> str:
    if value is None:
        return "–"
    return f"{value * 100:+.2f}" if signed else f"{value * 100:.2f}"


def _exposure(value: float | None) -> str:
    return "–" if value is None else f"{value:.3f}"


def _yes(value: bool | None) -> str:
    return "–" if value is None else "是" if value else "否"


def _table(lines: list[str], headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
    lines.extend(["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"])
    lines.extend("| " + " | ".join(row) + " |" for row in rows)


def _render_us(lines: list[str], us: Record | None) -> None:
    lines += ["### 美股大盘", ""]
    if us is None:
        lines.append("没有算到美股大盘的核对。")
        return
    lines.append(
        "- 长牛段和 TRADING-2565-34 的七段" + ("逐段一致。" if us["bulls_match"] else "对不上。")
    )
    gaps = [
        "–" if us["gaps"][key] is None else f"{us['gaps'][key]:.1e}"
        for key in ("exposure", "return", "benchmark")
    ]
    lines += [
        f"- 逐日对：起点 {us['start']}，评估台起点 {us['bench_start']}；"
        f"日期{'一样' if us['same_dates'] else '不一样'}；"
        f"最大差：策略敞口 {gaps[0]}、策略收益 {gaps[1]}、对照收益 {gaps[2]}。",
        "- 逐段对 TRADING-2565-34 印的段内收益和段内平均敞口（一位小数的百分数，"
        "线是半格 0.05 个百分点）；简单年化差的手算参照只印，不判（TRADING-2565-71）。",
        "",
    ]
    rows = []
    for i, (first, last) in enumerate(us["long_bulls"]):
        total = us["totals"][i] if i < len(us["totals"]) else None
        printed = total["printed"] if total is not None else None
        cells = []
        for j, key in enumerate(("strategy", "benchmark", "exposure")):
            reference = "–" if printed is None else f"{printed[j] * 100:.1f}%"
            value = "–" if total is None else f"{total[key] * 100:.2f}%"
            cells.append(f"{reference} / {value}")
        hand = us["hand"][i] if i < len(us["hand"]) else None
        simple = us["simple"][i] if i < len(us["simple"]) else None
        rows.append(
            [
                str(i + 1),
                first,
                last,
                *cells,
                _yes(total["match"] if total is not None else None),
                f"{_points(hand, signed=True)} / {_points(simple, signed=True)}",
            ]
        )
    _table(
        lines,
        (
            "段",
            "首月",
            "末月",
            "F0001 段内（-34 印的 / 重算）",
            "对照段内（-34 印的 / 重算）",
            "段内敞口（-34 印的 / 重算）",
            "一致",
            "简单年化差（手算参照 / 重算）",
        ),
        rows,
    )
    anchors = []
    for anchor in us["anchors"]:
        value = "–" if anchor["value"] is None else f"{anchor['value']:.10f}"
        difference = "–" if anchor["difference"] is None else f"{anchor['difference']:.1e}"
        anchors.append(
            f"{ANCHOR_TEXT[anchor['name']]} 重算 {value}，"
            f"-34 印的 {anchor['printed']:.10f}，差 {difference}"
        )
    lines += [
        "",
        f"- 简单年化差段间标准差：{_points(us['sd'])} 点；"
        f"手算参照 {_points(us['sd_reference'])} 点；一致：{_yes(us['sd_match'])}。",
        f"- 全精度核对（TRADING-2565-34 的 JSON，线 {TOLERANCE:.0e}）："
        + "；".join(anchors)
        + f"；一致：{_yes(us['anchors_match'])}。",
        f"- 整段平均敞口 {_exposure(us['exposure'])}、"
        f"长牛平均敞口 {_exposure(us['bull_exposure'])}；-34 印的 "
        f"{_exposure(us['printed_exposure'][0])}、{_exposure(us['printed_exposure'][1])}。",
        "- 结论：" + ("通过。" if us["passed"] else "没通过。"),
    ]


def render(result: Mapping[str, Any]) -> str:
    params = result["parameters"]
    lines = [
        f"# 在科技行业上直接读一次 F0001 的长牛代价（{result['run_id']}）",
        "",
        f"- 依据：`{result['owner_decision']}`，第 6 期章程第 17 节。",
        f"- 数据：信号快照 `{result['snapshot_id']}`，Hardw、Chips、Softw、Mkt-RF 和 RF；"
        f"只读到 {params['last_day']}，窗口 `{params['window']}`，"
        f"请求从 {params['first_day']} 起。",
        f"- 规则：F0001 是 QQQ-Q3 的 fast_slow，{params['rebalance']}，"
        f"两边成本 {params['cost_bps']:g} bps；目标、模拟、同平均敞口固定对照用评估台；"
        "划段和读数沿用 cycle_power、longbull_power。只读一次，不进台账、不算试验，k 不动。",
        "- 读数：长牛段内每日策略减对照的对数收益之和，除以日历年数（段的月数 / 12）；"
        "段间等权，three 按 Hardw、Chips、Softw 的顺序合并各段。",
        "",
        "## 判定（跑之前写死）",
        "",
    ]
    rows = []
    for variant in VARIANTS:
        item = result["variants"][variant]
        interval = f"{item['confidence']:g}：[{_points(item['low'], signed=True)}, "
        interval += f"{_points(item['high'], signed=True)}]"
        rows.append(
            [
                variant,
                str(item["segments"]),
                _points(item["mean"], signed=True),
                interval,
                VERDICT_TEXT[item["verdict"]],
                _yes(item["line"]),
            ]
        )
    _table(
        lines,
        ("变体", "段数", "平均读数（对数点/年）", "区间", "判定", "下界 > −1.67（只印）"),
        rows,
    )
    lines += [
        "",
        f"主读数 `three`：{VERDICT_TEXT[result['verdict']]}。{ACTION_TEXT[result['verdict']]}",
    ]
    if result["errors"]:
        lines += ["", "## 出错", "", *[f"- {error}" for error in result["errors"]]]
    lines += ["", "## 核对（读科技行业之前）", ""]
    _render_us(lines, result["checks"]["us"])
    lines += ["", "### 划段（和 TRADING-2565-63 印的比）", ""]
    tech = result["checks"]["tech"]
    if tech is None:
        lines.append("没有算到科技序列的划段核对。")
    else:
        _table(
            lines,
            ("序列", "长牛段", "一致", "去掉的缺值日"),
            [
                [
                    name,
                    "、".join(key.removeprefix("long_bull_") for key in row["found"]),
                    _yes(row["match"]),
                    "、".join(row["dropped"]) or "无",
                ]
                for name, row in tech["series"].items()
            ],
        )
    if result["series"]:
        lines += ["", "## 只印", ""]
        _table(
            lines,
            ("变体", "段间 t 标准误（点）", "和 -63 埋规则之比", "简单年化差段间标准差（点）"),
            [
                [
                    variant,
                    _points(item["se"]),
                    "–" if item["ratio"] is None else f"{item['ratio']:.2f}",
                    _points(item["simple_sd"]),
                ]
                for variant, item in result["variants"].items()
            ],
        )
        lines += [
            "",
            "口径不同，比值偏低：TRADING-2565-63 的 1.92、1.49 是 500 条历史上点估计的标准差，"
            "这里只有一条路径的段间 t 标准误，它比真实的窄（规格第 5 节）。",
            "",
        ]
        _table(
            lines,
            ("序列", "数据起", "窗口起点", "第一个收益日", "天数", "整段平均敞口"),
            [
                [
                    name,
                    item["first_day"],
                    item["start"],
                    item["first_return"],
                    str(item["days"]),
                    _exposure(item["exposure"]),
                ]
                for name, item in result["series"].items()
            ],
        )
        for name, item in result["series"].items():
            lines += ["", f"### {name} 的长牛段", ""]
            _table(
                lines,
                (
                    "段",
                    "首月",
                    "末月",
                    "月数",
                    "读数",
                    "简单年化差",
                    "段内平均敞口",
                    "整段平均敞口",
                ),
                [
                    [
                        row["id"],
                        row["start"],
                        row["end"],
                        str(row["months"]),
                        _points(row["reading"], signed=True),
                        _points(row["simple"], signed=True),
                        _exposure(row["exposure"]),
                        _exposure(row["whole"]),
                    ]
                    for row in item["long_bulls"]
                ],
            )
            lines += ["", f"### {name} 的慢熊段（F0001 省了多少，只印）", ""]
            _table(
                lines,
                ("段", "首月", "末月", "F0001 段内收益", "对照段内收益", "差", "段内平均敞口"),
                [
                    [
                        row["id"],
                        row["start"],
                        row["end"],
                        _points(row["strategy"], signed=True),
                        _points(row["benchmark"], signed=True),
                        _points(row["difference"], signed=True),
                        _exposure(row["exposure"]),
                    ]
                    for row in item["slow_bears"]
                ],
            )
            if item["uncovered"]:
                lines += ["", "没覆盖（F0001 还没开始）：" + "、".join(item["uncovered"]) + "。"]
    lines += ["", "production_effect=none；broker_action=none。", ""]
    return "\n".join(lines)


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / SUMMARY).write_text(
        json.dumps(result, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / REPORT).write_text(render(result), encoding="utf-8")


def load_inputs(root: Path = PROJECT_ROOT) -> Inputs:
    snapshot = signal_sources.load_snapshot(root, cp.SNAPSHOT_ID)
    industries = cp.cut(snapshot.frames[cp.INDUSTRIES]).loc[:, list(cp.COLUMNS)]
    factors = cp.cut(snapshot.frames[cp.FACTORS]).loc[:, [MARKET, cp.CASH]]
    loaded = load_settings()
    gate, plan = tr.load_trial(lg.research_root(loaded), gp.TRIAL_ID)
    if plan.data.signal_snapshot_id != cp.SNAPSHOT_ID:
        raise ValueError(f"{gp.TRIAL_ID} does not use {cp.SNAPSHOT_ID}")
    bench_us = gp.cut(
        tr.market_data("us_market", gate, plan, snapshot_root=root, project_root=root), cp.LAST_DAY
    )
    window = resolve_window(loaded, gate.markets["us_market"].question_type, "development")
    if (window.start, window.end) != (cp.FIRST_DAY, cp.LAST_DAY):
        raise ValueError(f"{window.window_id} is not {cp.FIRST_DAY}..{cp.LAST_DAY}")
    entry = backfill_check.source_entry(lg.research_root(loaded), backfill_check.CONFIGS[0])
    return Inputs(industries, factors, bench_us, window, entry, loaded)


def _progress(name: str) -> None:
    typer.echo(f"longbull_tech_read: 读完 {name}")


@app.callback()
def main() -> None:
    """先核对，再在科技行业上直接读一次 F0001 的长牛代价。"""


@app.command("run")
def run_command(out: Annotated[Path, typer.Option(help="结果目录。")]) -> None:
    """全部读完后才写文件，核对失败不读科技行业。"""
    try:
        inputs = load_inputs()
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"longbull_tech_read: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None
    typer.echo(
        f"longbull_tech_read: 数据已载入并截到 {cp.LAST_DAY}；"
        f"美股大盘 {len(inputs.bench_us.returns)} 天，行业 {len(inputs.industries)} 天"
    )
    result = read(inputs, progress=_progress)
    write_result(result, out)
    checks = result["checks"]
    us_ok = checks["us"] is not None and checks["us"]["passed"]
    tech_ok = checks["tech"] is not None and checks["tech"]["passed"]
    typer.echo(
        "longbull_tech_read: 美股大盘核对"
        + ("通过" if us_ok else "没通过")
        + "；划段核对"
        + ("通过" if tech_ok else "没通过")
    )
    if checks["tech"] is not None:
        typer.echo(
            "longbull_tech_read: 去掉的缺值日 "
            + "、".join(
                f"{name} {len(row['dropped'])} 天" for name, row in checks["tech"]["series"].items()
            )
        )
    for error in result["errors"]:
        typer.echo(f"longbull_tech_read: 出错 {error}")
    for variant in VARIANTS:
        item = result["variants"][variant]
        typer.echo(
            f"longbull_tech_read: {variant} 判定 {item['verdict']}，"
            f"平均读数 {_points(item['mean'], signed=True)} 点"
        )
    typer.echo(f"longbull_tech_read: 写入 {out / SUMMARY} 和 {out / REPORT}")


if __name__ == "__main__":
    app()
