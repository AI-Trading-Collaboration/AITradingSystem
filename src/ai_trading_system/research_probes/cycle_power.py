"""科技类行业整轮尺子的检验力探针（TRADING-2565-61）。

依据 owner_decision:TRADING-2565:2026-10-23:tech_proxy_power_probe、第 6 期章程第 13 节。
只用埋的规则，故意前视；不进任何账本、不算试验，k 不动。
F0001 和 fast_slow 等已有配置不在这里跑；不改门，不授予生产或 broker 权限。

数据只取已检查信号快照的 Hardw、Chips、Softw 和 RF，窗口为 1929–2010。
三列有值者等权为 tech_ew；慢熊划段直接复用评估台，从上一个谷底次月到本次谷底
为一个周期，最后的尾巴不抽。有放回抽周期，接成 500 条 984 个月的合成历史。
每个市场在合成历史上重新划段，埋入慢熊延迟退出和再入、以及随机三个月假退出；
两种假退出频率共用随机数，plant_base 判，plant_noisy 只印。
读数是策略对同平均开盘敞口基准的日对数收益差，按有值天数合并并乘 252；
零成本用于判定，25 bps 只印。平稳块 bootstrap 的九把尺子共用抽样。
以历史平均读数为中心平移到每年 −2、−1、0、+1、+2 对数点，
先约束零效果误判和反号，再按 ±2 的较低检验力选尺子；并列取长块、高置信度。
检验力至少 80% 为 ready，至少 50% 为 large_only，其余 below_50；
没有合格尺子为 none_eligible，周期不足或任何历史出错为 incomplete。
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import segments
from ai_trading_system.research_bench.settings import Window
from ai_trading_system.research_data import signal_sources

# 需求第 2–5 节，跑之前写死。
RUN_ID = "TRADING-2565-61"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-23:tech_proxy_power_probe"
SNAPSHOT_ID = "sig-7ad8aa00b736"
INDUSTRIES, FACTORS = "ff_49_industries_daily", "ff_factors_daily"
COLUMNS = ("Hardw", "Chips", "Softw")
CASH, TECH = "RF", "tech_ew"
MARKETS = (TECH, *COLUMNS)
FIRST_DAY, LAST_DAY = date(1929, 1, 2), date(2010, 12, 31)
FIRST_MONTH = pd.Period("1929-01", freq="M")
MONTHS, HISTORIES, DRAWS = 984, 500, 1000
SEED = 20261024
YEAR_DAYS = 252
BLOCKS = (21, 252, 1260)
CONFIDENCES = (0.95, 0.99, 0.997)
LEVELS = (-0.02, -0.01, 0.0, 0.01, 0.02)
VARIANTS = (TECH, "three")
HIGH, LOW = 1.0, 0.25
EXIT_MONTHS, REENTRY_MONTHS, FALSE_MONTHS = 1, 3, 3
PLANTS = {"plant_base": 1.0, "plant_noisy": 2.0}
JUDGED_PLANT = "plant_base"
COST_BPS = 25.0
FALSE_LIMIT, WILSON_LIMIT, SIGN_LIMIT = 0.05, 0.075, 0.01
READY, LARGE, MIN_CYCLES = 0.8, 0.5, 8
WILSON_Z = 1.959963984540054
ESTIMATE_SD = (0.0077, 0.0110)
JOURNAL, SUMMARY, REPORT = "histories.jsonl", "summary.json", "report.md"

Array = np.ndarray[Any, Any]
Blocks = Mapping[int, tuple[Array, Array]]
Record = dict[str, Any]

app = typer.Typer()


def cut(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[pd.DatetimeIndex(frame.index) <= pd.Timestamp(LAST_DAY)]


def market_frame(industries: pd.DataFrame, factors: pd.DataFrame) -> pd.DataFrame:
    industries, factors = cut(industries), cut(factors)
    missing = [name for name in COLUMNS if name not in industries]
    if CASH not in factors:
        missing.append(CASH)
    if missing:
        raise ValueError(f"missing columns: {missing}")
    tech = segments.window_returns(
        industries.loc[:, list(COLUMNS)].mean(axis=1),
        Window(f"{TECH}_1929_2010", "development", FIRST_DAY, LAST_DAY),
    )
    cash = factors[CASH].reindex(tech.index)
    if cash.isna().any():
        when = pd.Timestamp(cash.index[cash.isna()][0]).date()
        raise ValueError(f"missing RF on {when}")
    result = industries.loc[:, list(COLUMNS)].reindex(tech.index).copy()
    result.insert(0, TECH, tech)
    result[CASH] = cash
    return result


def load_frame(root: Path = PROJECT_ROOT) -> pd.DataFrame:
    snapshot = signal_sources.load_snapshot(root, SNAPSHOT_ID)
    return market_frame(snapshot.frames[INDUSTRIES], snapshot.frames[FACTORS])


@dataclass(frozen=True)
class Cycle:
    id: str
    start: str
    end: str
    peak: str
    change: float
    months: int


def cycles(frame: pd.DataFrame) -> tuple[list[Cycle], list[str]]:
    level = segments.monthly_level(frame[TECH])
    months = pd.PeriodIndex(level.index, freq="M")
    if months.empty:
        return [], []
    begin = months[0]
    found: list[Cycle] = []
    for item in segments.drop_segments(level):
        if item.kind != "slow_bear" or item.peak is None or item.trough is None:
            continue
        trough = pd.Period(item.trough, freq="M")
        if item.change is None:
            raise ValueError("slow bear has no change")
        found.append(
            Cycle(
                f"cycle_{len(found) + 1:02d}",
                str(begin),
                str(trough),
                item.peak,
                item.change,
                trough.ordinal - begin.ordinal + 1,
            )
        )
        begin = trough + 1
    return found, [str(m) for m in months if m >= begin]


@dataclass(frozen=True)
class History:
    frame: pd.DataFrame
    drawn: tuple[str, ...]


def history(
    frame: pd.DataFrame,
    found: Sequence[Cycle],
    rng: np.random.Generator,
    months: int = MONTHS,
) -> History:
    if not found:
        raise ValueError("no cycles")
    if months < 1 or any(c.months < 1 for c in found):
        raise ValueError("months must be positive")
    calendar = pd.DatetimeIndex(frame.index).to_period("M")
    rows: list[pd.DataFrame] = []
    drawn: list[str] = []
    joined = 0
    while joined < months:
        cycle = found[int(rng.integers(0, len(found)))]
        first, last = pd.Period(cycle.start, freq="M"), pd.Period(cycle.end, freq="M")
        inside = (calendar >= first) & (calendar <= last)
        part = frame.loc[inside, [TECH, *COLUMNS, CASH]].copy()
        ordinals = calendar[inside].astype("int64").to_numpy(dtype=np.int64)
        part["month"] = joined + ordinals - first.ordinal
        rows.append(part)
        drawn.append(cycle.id)
        joined += cycle.months
    result = pd.concat(rows, ignore_index=True)
    result = result.loc[result["month"] < months].reset_index(drop=True)
    return History(result, tuple(drawn))


@dataclass(frozen=True)
class Market:
    risky: Array
    cash: Array
    month: Array
    rows: Array


def market(frame: pd.DataFrame, name: str) -> Market:
    rows = np.flatnonzero(frame[name].notna().to_numpy())
    part = frame.iloc[rows]
    _, month = np.unique(part["month"].to_numpy(), return_inverse=True)
    return Market(part[name].to_numpy(dtype=float), part[CASH].to_numpy(dtype=float), month, rows)


def month_level(risky: Array, month: Array) -> pd.Series:
    logs = np.bincount(month, weights=np.log1p(risky))
    return pd.Series(
        np.exp(np.cumsum(logs)), index=pd.period_range(FIRST_MONTH, periods=len(logs), freq="M")
    )


def slow_bears(level: pd.Series) -> list[tuple[int, int]]:
    if level.empty:
        return []
    first = pd.Period(level.index[0], freq="M").ordinal
    return [
        (
            pd.Period(s.peak, freq="M").ordinal - first,
            pd.Period(s.trough, freq="M").ordinal - first,
        )
        for s in segments.drop_segments(level)
        if s.kind == "slow_bear" and s.peak is not None and s.trough is not None
    ]


def plant_targets(
    bears: Sequence[tuple[int, int]], count: int, uniforms: Array, rate: float
) -> Array:
    if len(uniforms) != count - 1:
        raise ValueError("uniforms must have count - 1 entries")
    slow_low = np.zeros(count, dtype=bool)
    for peak, trough in bears:
        slow_low[peak + EXIT_MONTHS + 1 : trough + REENTRY_MONTHS + 1] = True
    low = slow_low.copy()
    for m in np.flatnonzero(~slow_low[:-1] & (uniforms < rate / 12)):
        low[m + 1 : m + FALSE_MONTHS + 1] = True
    return np.where(low, LOW, HIGH)


def simulate(
    risky: Array, cash: Array, month: Array, targets: Array, cost_bps: float = 0.0
) -> tuple[Array, Array]:
    """按月重置两条腿的累计对数收益；开盘敞口先于当日收益，成本不改变权重。"""
    if not len(risky):
        return np.empty(0), np.empty(0)
    starts = np.r_[0, np.flatnonzero(month[1:] != month[:-1]) + 1]
    lengths = np.diff(np.r_[starts, len(month)])
    weights = targets[month]

    def legs(returns: Array) -> Array:
        cumulative = np.r_[0.0, np.cumsum(np.log1p(returns))]
        return np.asarray(np.exp(cumulative[1:] - np.repeat(cumulative[starts], lengths)))

    risky_leg, cash_leg = weights * legs(risky), (1.0 - weights) * legs(cash)
    value = risky_leg + cash_leg
    prior_value = np.r_[1.0, value[:-1]]
    prior_value[starts] = 1.0
    gross = value / prior_value
    close = risky_leg / value
    held = np.r_[0.0, close[:-1]]
    held[starts] = weights[starts]
    if cost_bps:
        trades = starts[1:]
        gross[trades] *= 1.0 - cost_bps / 10000 * np.abs(weights[trades] - close[trades - 1])
    return gross, held


def block_draws(
    length: int, draws: int, mean_block: float, rng: np.random.Generator
) -> tuple[Array, Array]:
    if length < 1 or draws < 1 or mean_block < 1:
        raise ValueError("length, draws and mean_block must be at least 1")
    p = 1.0 / mean_block
    width = math.ceil(length * p + 6 * math.sqrt(length * p) + 10)
    starts = rng.integers(0, length, size=(draws, width))
    sizes = rng.geometric(p, size=(draws, width))
    while np.any(sizes.sum(axis=1) < length):
        starts = np.concatenate((starts, rng.integers(0, length, size=(draws, width))), axis=1)
        sizes = np.concatenate((sizes, rng.geometric(p, size=(draws, width))), axis=1)
    before = np.cumsum(sizes, axis=1) - sizes
    sizes = np.minimum(sizes, np.maximum(length - before, 0))
    return starts, sizes


def block_sums(values: Array, starts: Array, sizes: Array) -> Array:
    cumulative = np.r_[0.0, np.cumsum(np.concatenate((values, values)))]
    return np.asarray((cumulative[starts + sizes] - cumulative[starts]).sum(axis=1))


def expand(starts: Array, sizes: Array, length: int) -> Array:
    """仅供测试逐日核对；运行的统计路径只用块累计和。"""
    paths = np.empty((len(starts), length), dtype=int)
    for row in range(len(starts)):
        paths[row] = np.concatenate(
            [
                (start + np.arange(size)) % length
                for start, size in zip(starts[row], sizes[row], strict=True)
                if size
            ]
        )
    return paths


def reading(numerator: Array, count: Array) -> float:
    return float(YEAR_DAYS * numerator.sum() / count.sum())


def bounds(
    numerator: Array,
    count: Array,
    blocks: Blocks,
    confidences: Sequence[float] = CONFIDENCES,
) -> dict[str, dict[str, list[float]]]:
    out: dict[str, dict[str, list[float]]] = {}
    for block, (starts, sizes) in blocks.items():
        values = YEAR_DAYS * block_sums(numerator, starts, sizes) / block_sums(count, starts, sizes)
        out[str(block)] = {}
        for confidence in confidences:
            tail = (1.0 - confidence) / 2.0
            out[str(block)][f"{confidence:g}"] = [
                float(x) for x in np.quantile(values, [tail, 1.0 - tail])
            ]
    return out


def judge(low: float, high: float) -> str:
    return "gain" if low > 0 else "loss" if high < 0 else "undecided"


def wilson_upper(hits: int, total: int, z: float = WILSON_Z) -> float:
    if total < 1:
        raise ValueError("total must be positive")
    share = hits / total
    return (
        share
        + z * z / (2 * total)
        + z * math.sqrt(share * (1 - share) / total + z * z / (4 * total**2))
    ) / (1 + z * z / total)


def evaluate_history(
    frame: pd.DataFrame,
    found: Sequence[Cycle],
    seed: int,
    index: int,
    *,
    months: int = MONTHS,
    draws: int = DRAWS,
    blocks: Sequence[int] = BLOCKS,
    confidences: Sequence[float] = CONFIDENCES,
) -> Record:
    rng = np.random.default_rng([seed, index])
    made = history(frame, found, rng, months)
    days = len(made.frame)
    counts = {v: np.zeros(days) for v in VARIANTS}
    numerators = {v: {p: np.zeros(days) for p in PLANTS} for v in VARIANTS}
    costs = {v: np.zeros(days) for v in VARIANTS}
    bears_count: dict[str, int] = {}
    matched_exposure: dict[str, float] = {}
    for name in MARKETS:
        part = market(made.frame, name)
        if not len(part.rows):
            continue
        level = month_level(part.risky, part.month)
        bears = slow_bears(level)
        bears_count[name] = len(bears)
        uniforms = rng.random(len(level) - 1)
        variant = TECH if name == TECH else "three"
        counts[variant][part.rows] += 1
        for plant, rate in PLANTS.items():
            targets = plant_targets(bears, len(level), uniforms, rate)
            gross, held = simulate(part.risky, part.cash, part.month, targets)
            exposure = float(held.mean())
            matched = np.full(len(level), exposure)
            reference, _ = simulate(part.risky, part.cash, part.month, matched)
            numerators[variant][plant][part.rows] += np.log(gross) - np.log(reference)
            if plant == JUDGED_PLANT:
                matched_exposure[name] = exposure
                costly, _ = simulate(part.risky, part.cash, part.month, targets, COST_BPS)
                reference, _ = simulate(part.risky, part.cash, part.month, matched, COST_BPS)
                costs[variant][part.rows] += np.log(costly) - np.log(reference)
    sampled = {block: block_draws(days, draws, block, rng) for block in blocks}
    return {
        "history": index,
        "drawn": list(made.drawn),
        "days": days,
        "slow_bears": bears_count,
        "matched_exposure": matched_exposure,
        "point": {v: {p: reading(numerators[v][p], counts[v]) for p in PLANTS} for v in VARIANTS},
        "cost_25": {v: reading(costs[v], counts[v]) for v in VARIANTS},
        "bounds": {
            v: {p: bounds(numerators[v][p], counts[v], sampled, confidences) for p in PLANTS}
            for v in VARIANTS
        },
        "error": None,
    }


def failed_history(index: int, error: str) -> Record:
    return {"history": index, "error": error}


def level_key(level: float) -> str:
    return f"{round(level * 100):+d}"


def ruler_rows(
    records: Sequence[Record],
    variant: str,
    plant: str,
    *,
    levels: Sequence[float] = LEVELS,
    blocks: Sequence[int] = BLOCKS,
    confidences: Sequence[float] = CONFIDENCES,
) -> Record:
    points = [r["point"][variant][plant] for r in records]
    centre = float(np.mean(points))
    rows = []
    total = len(records)
    for block in blocks:
        for confidence in confidences:
            shares = {}
            zero_hits = 0
            for level in levels:
                shift = level - centre
                decisions = [
                    judge(low + shift, high + shift)
                    for r in records
                    for low, high in [r["bounds"][variant][plant][str(block)][f"{confidence:g}"]]
                ]
                hits = Counter(decisions)
                shares[level_key(level)] = {s: hits[s] / total for s in ("gain", "loss")}
                if level == 0:
                    zero_hits = hits["gain"] + hits["loss"]
            false_rate = zero_hits / total
            upper = wilson_upper(zero_hits, total)
            reverse = max(shares["+2"]["loss"], shares["-2"]["gain"])
            rows.append(
                {
                    "block": block,
                    "confidence": confidence,
                    "shares": shares,
                    "false_rate": false_rate,
                    "wilson_upper": upper,
                    "reverse_rate": reverse,
                    "score": min(shares["+2"]["gain"], shares["-2"]["loss"]),
                    "power_1": min(shares["+1"]["gain"], shares["-1"]["loss"]),
                    "eligible": false_rate <= FALSE_LIMIT
                    and upper <= WILSON_LIMIT
                    and reverse <= SIGN_LIMIT,
                }
            )
    return {
        "centre": centre,
        "spread": float(np.std(points, ddof=1)) if total > 1 else None,
        "cost_25": float(np.mean([r["cost_25"][variant] for r in records])),
        "rows": rows,
    }


def choose(rows: Sequence[Record]) -> Record:
    eligible = [r for r in rows if r["eligible"]]
    if not eligible:
        return {"verdict": "none_eligible", "block": None, "confidence": None, "score": None}
    best = max(eligible, key=lambda r: (r["score"], r["block"], r["confidence"]))
    verdict = (
        "ready"
        if best["score"] >= READY
        else "large_only"
        if best["score"] >= LARGE
        else "below_50"
    )
    return {"verdict": verdict, **{k: best[k] for k in ("block", "confidence", "score")}}


def bear_spread(records: Sequence[Record]) -> Record:
    out = {}
    for name in MARKETS:
        values = [r["slow_bears"][name] for r in records if name in r["slow_bears"]]
        counts = Counter(values)
        out[name] = {
            "histories": len(values),
            "mean": float(np.mean(values)) if values else None,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "counts": {str(k): counts[k] for k in sorted(counts)},
        }
    return out


def run_probe(
    frame: pd.DataFrame,
    *,
    histories: int = HISTORIES,
    draws: int = DRAWS,
    seed: int = SEED,
    months: int = MONTHS,
    blocks: Sequence[int] = BLOCKS,
    confidences: Sequence[float] = CONFIDENCES,
    journal: Path | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> Record:
    found, tail = cycles(frame)
    records = []
    if journal is not None:
        journal.write_text("", encoding="utf-8")
    if len(found) >= MIN_CYCLES:
        for index in range(histories):
            try:
                record = evaluate_history(
                    frame,
                    found,
                    seed,
                    index,
                    months=months,
                    draws=draws,
                    blocks=blocks,
                    confidences=confidences,
                )
            except Exception as exc:  # 保留每条失败；任何一条失败都禁止给出完整判定。
                record = failed_history(index, f"{type(exc).__name__}: {exc}".splitlines()[0])
            records.append(record)
            if journal is not None:
                with journal.open("a", encoding="utf-8") as sink:
                    sink.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            if progress is not None:
                progress(index + 1, histories)
    errors = [
        {"history": r["history"], "error": r["error"]} for r in records if r["error"] is not None
    ]
    good = [r for r in records if r["error"] is None]
    variants = {}
    for variant in VARIANTS:
        tables = (
            {
                p: ruler_rows(good, variant, p, blocks=blocks, confidences=confidences)
                for p in PLANTS
            }
            if good
            else {}
        )
        read = (
            {"verdict": "incomplete", "block": None, "confidence": None, "score": None}
            if len(found) < MIN_CYCLES or errors or not good
            else choose(tables[JUDGED_PLANT]["rows"])
        )
        variants[variant] = {"reading": read, "tables": tables}
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": SNAPSHOT_ID,
        "parameters": {
            "months": months,
            "histories": histories,
            "draws": draws,
            "seed": seed,
            "blocks": list(blocks),
            "confidences": list(confidences),
            "levels": list(LEVELS),
            "plants": dict(PLANTS),
        },
        "cycles": [asdict(c) for c in found],
        "tail": {"first": tail[0], "last": tail[-1], "months": len(tail)} if tail else None,
        "errors": errors,
        "slow_bear_counts": bear_spread(good),
        "variants": variants,
    }


VERDICT_TEXT = {
    "ready": "±2 检验力至少 80%，尺子可用；不代表真实规则有效或获准运行。",
    "large_only": "±2 检验力至少 50% 但不到 80%，只能量较大的效果。",
    "below_50": "±2 检验力不到 50%，量不清整轮赚亏。",
    "none_eligible": "没有合格的尺子，不能据此判整轮赚亏。",
    "incomplete": "周期不足、历史出错或没有成功历史；先查原因，不作完整判定。",
}


def _pct(value: float | None) -> str:
    return "–" if value is None else f"{value:.1%}"


def _points(value: float | None) -> str:
    return "–" if value is None else f"{value * 100:+.2f}"


def render(result: Mapping[str, Any]) -> str:
    lines = [
        f"# 科技类行业整轮尺子的检验力探针（{RUN_ID}）",
        "",
        f"- 依据：`{OWNER_DECISION}`（第 6 期章程第 13 节）。",
        f"- 数据：信号快照 `{SNAPSHOT_ID}`，Hardw、Chips、Softw 和 RF，只读到 {LAST_DAY}。",
        "- 只用埋的规则；F0001、fast_slow 和别的已有配置都没有跑。不进台账、不算试验，k 不动。",
        "- 读数是成对日对数收益差的平均乘 252，单位是对数点/年；成本 0 是判的口径，25 bps 只印。",
        "",
        "## 判定",
        "",
        "| 变体 | 判定 | 中文说明 | 尺子（块长 / 置信度） | 分数 |",
        "|---|---|---|---|---|",
    ]
    for variant, item in result["variants"].items():
        read = item["reading"]
        ruler = "–" if read["block"] is None else f"{read['block']} / {read['confidence']:g}"
        lines.append(
            f"| {variant} | {read['verdict']} | {VERDICT_TEXT[read['verdict']]} | "
            f"{ruler} | {_pct(read['score'])} |"
        )
    lines += [
        "",
        "## 周期",
        "",
        f"{TECH} 的周期 {len(result['cycles'])} 个；尾巴不抽，周期有放回抽样。",
        "",
        "| 周期 | 起月 | 谷底月 | 高点月 | 跌幅 | 月数 |",
        "|---|---|---|---|---|---|",
    ]
    for cycle in result["cycles"]:
        lines.append(
            f"| {cycle['id']} | {cycle['start']} | {cycle['end']} | {cycle['peak']} | "
            f"{_pct(cycle['change'])} | {cycle['months']} |"
        )
    tail = result["tail"]
    lines += [
        "",
        (
            f"尾巴（不抽）：{tail['first']}..{tail['last']}，{tail['months']} 个月"
            if tail
            else "尾巴（不抽）：无"
        ),
    ]
    if result["errors"]:
        lines += ["", "## 出错的历史", ""]
        for error in result["errors"]:
            lines.append(f"- {error['history']}：{error['error']}")
    for variant, item in result["variants"].items():
        for plant, table in item["tables"].items():
            role = "判的口径" if plant == JUDGED_PLANT else "只印"
            lines += [
                "",
                f"## {variant} · {plant}（{role}）",
                "",
                f"Δg₀ {_points(table['centre'])}；Δg_h 的标准差 {_points(table['spread'])}"
                f"（估算对照 {_points(ESTIMATE_SD[0])}..{_points(ESTIMATE_SD[1])}）；"
                f"25 bps 的 Δg₀ {_points(table['cost_25'])}。",
                "",
                "| 块长 | 置信度 | 误判率 | Wilson 上限 | 反号率 | ±2 检验力 | ±1 检验力 | 合格 |",
                "|---|---|---|---|---|---|---|---|",
            ]
            for row in table["rows"]:
                rates = " | ".join(
                    _pct(row[k])
                    for k in ("false_rate", "wilson_upper", "reverse_rate", "score", "power_1")
                )
                lines.append(
                    f"| {row['block']} | {row['confidence']:g} | {rates} | "
                    f"{'是' if row['eligible'] else '否'} |"
                )
            levels = result["parameters"]["levels"]
            lines += [
                "",
                "各档判赚 / 判亏的占比：",
                "",
                "| 块长 | 置信度 | " + " | ".join(level_key(v) for v in levels) + " |",
                "|---|---|" + "---|" * len(levels),
            ]
            for row in table["rows"]:
                shares = row["shares"]
                cells = " | ".join(
                    f"{_pct(shares[level_key(v)]['gain'])} / {_pct(shares[level_key(v)]['loss'])}"
                    for v in levels
                )
                lines.append(f"| {row['block']} | {row['confidence']:g} | {cells} |")
    lines += [
        "",
        "## 重新划出的慢熊段数（每条历史）",
        "",
        "| 市场 | 历史条数 | 平均 | 最少 | 最多 | 段数：条数 |",
        "|---|---|---|---|---|---|",
    ]
    for name, spread in result["slow_bear_counts"].items():
        mean = "–" if spread["mean"] is None else f"{spread['mean']:.2f}"
        least = "–" if spread["min"] is None else str(spread["min"])
        most = "–" if spread["max"] is None else str(spread["max"])
        counts = "、".join(f"{k}：{v}" for k, v in spread["counts"].items()) or "–"
        lines.append(f"| {name} | {spread['histories']} | {mean} | {least} | {most} | {counts} |")
    lines += ["", "production_effect=none；broker_action=none。", ""]
    return "\n".join(lines)


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / SUMMARY).write_text(
        json.dumps(result, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / REPORT).write_text(render(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """科技类行业整轮尺子的检验力探针，只用埋的规则。"""


@app.command("run")
def run_command(out: Annotated[Path, typer.Option(help="结果目录。")]) -> None:
    """抽慢熊周期合成历史，量整轮尺子的检验力，不运行真实规则。"""
    try:
        frame = load_frame()
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"cycle_power: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None
    found, _ = cycles(frame)
    typer.echo(f"cycle_power: 数据已载入并截到 {LAST_DAY}；{len(frame)} 天；周期 {len(found)} 个")
    out.mkdir(parents=True, exist_ok=True)

    def progress(done: int, total: int) -> None:
        if done % 25 == 0 or done == total:
            typer.echo(f"cycle_power: {done}/{total}")

    result = run_probe(
        frame,
        histories=HISTORIES,
        draws=DRAWS,
        months=MONTHS,
        journal=out / JOURNAL,
        progress=progress,
    )
    write_result(result, out)
    for variant, item in result["variants"].items():
        read = item["reading"]
        typer.echo(f"cycle_power: {variant} 判定 {read['verdict']}，分数 {_pct(read['score'])}")
    typer.echo(f"cycle_power: 写入 {out / SUMMARY} 和 {out / REPORT}")


if __name__ == "__main__":
    app()
