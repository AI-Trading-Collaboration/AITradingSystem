"""科技行业长牛代价的检验力探针（TRADING-2565-63）。

依据 owner_decision:TRADING-2565:2026-10-24:longbull_cost_probe、第 6 期章程第 14 节。
只用埋的规则，故意前视；不进任何账本、不算试验，k 不动。
F0001 和 fast_slow 等已有配置不在这里跑；cycle_power 不改，只导入复用。
美股大盘的合成历史只定标假退出频率，让段间简单年化差的标准差接近 1.24 点，
不印、不判代价大小。科技行业沿用相同种子的周期抽样，按合成历史重新划长牛段。
读数为每段日对数差之和除以日历年数，段间等权；三行业变体合并各列的段。
以历史平均读数为中心，把区间平移到 +1、0、−1、−1.67、−2.5、−3.34 对数点。
段间 t 和段自助法各三档置信度；先约束零效果误判、Wilson 上限和反号，
再按 −2.5 的检验力选尺子，并列取 t、高置信度。80% 为 ready，50% 为 large_only，
其余 below_50；没有合格尺子为 none_eligible，周期不足或任何历史出错为 incomplete。
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import segments
from ai_trading_system.research_bench.settings import Window
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_probes import cycle_power as cp

# 需求第 2–5 节，跑之前写死。
RUN_ID = "TRADING-2565-63"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-24:longbull_cost_probe"
MARKET, US = "Mkt-RF", "us_market"
MONTHS, HISTORIES, DRAWS, SEED = cp.MONTHS, cp.HISTORIES, cp.DRAWS, cp.SEED
RATES = (0.25, 0.5, 1.0)
TARGET_SD = 0.0124
LEVELS = (0.01, 0.0, -0.01, -0.0167, -0.025, -0.0334)
POWER_LEVEL, PRINT_LINE = -0.025, -0.0167
RULERS = ("t_seg", "seg_boot")
MIN_SEGMENTS = 2
CALIBRATION, JOURNAL, SUMMARY, REPORT = (
    "calibration.jsonl",
    "histories.jsonl",
    "summary.json",
    "report.md",
)

Array = np.ndarray[Any, Any]
Record = dict[str, Any]
Spans = Sequence[tuple[int, int]]
Bounds = dict[str, dict[str, list[float]]]

app = typer.Typer()


def rate_key(rate: float) -> str:
    return f"{rate:g}"


def level_key(level: float) -> str:
    return f"{level * 100:+g}"


def us_frame(factors: pd.DataFrame) -> pd.DataFrame:
    factors = cp.cut(factors)
    missing = [name for name in (MARKET, cp.CASH) if name not in factors]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    daily = segments.window_returns(
        factors[MARKET] + factors[cp.CASH],
        Window(f"{US}_1929_2010", "development", cp.FIRST_DAY, cp.LAST_DAY),
    )
    cash = factors[cp.CASH].reindex(daily.index)
    if cash.isna().any():
        when = pd.Timestamp(cash.index[cash.isna()][0]).date()
        raise ValueError(f"missing RF on {when}")
    result = pd.DataFrame({cp.TECH: daily})
    for name in cp.COLUMNS:
        result[name] = np.nan
    result[cp.CASH] = cash
    return result


def load_frames(root: Path = PROJECT_ROOT) -> tuple[pd.DataFrame, pd.DataFrame]:
    snapshot = signal_sources.load_snapshot(root, cp.SNAPSHOT_ID)
    industries, factors = snapshot.frames[cp.INDUSTRIES], snapshot.frames[cp.FACTORS]
    return cp.market_frame(industries, factors), us_frame(factors)


def long_bulls(level: pd.Series) -> list[tuple[int, int]]:
    if level.empty:
        return []
    first = pd.Period(level.index[0], freq="M").ordinal
    return [
        (
            pd.Period(item.start, freq="M").ordinal - first,
            pd.Period(item.end, freq="M").ordinal - first,
        )
        for item in segments.long_bull_segments(level, segments.drop_segments(level))
    ]


def span_years(spans: Spans) -> Array:
    return np.asarray([(last - first + 1) / 12 for first, last in spans], dtype=float)


def span_sums(values: Array, month: Array, spans: Spans) -> tuple[Array, Array]:
    sums = np.empty(len(spans), dtype=float)
    counts = np.empty(len(spans), dtype=int)
    for i, (first, last) in enumerate(spans):
        inside = (month >= first) & (month <= last)
        sums[i] = values[inside].sum()
        counts[i] = inside.sum()
    return sums, counts


def readings(log_difference: Array, month: Array, spans: Spans) -> Array:
    sums, _ = span_sums(log_difference, month, spans)
    return np.asarray(sums / span_years(spans))


def simple_differences(strategy: Array, benchmark: Array, month: Array, spans: Spans) -> Array:
    own, _ = span_sums(strategy, month, spans)
    reference, _ = span_sums(benchmark, month, spans)
    years = span_years(spans)
    return np.asarray(np.exp(own / years) - np.exp(reference / years))


def shift_long_bulls(gross: Array, month: Array, spans: Spans, delta: float) -> Array:
    moved = gross.copy()
    for (first, last), years in zip(spans, span_years(spans), strict=True):
        inside = (month >= first) & (month <= last)
        count = int(inside.sum())
        if count:
            moved[inside] *= math.exp(delta * years / count)
    return moved


def sample_sd(values: Array) -> float | None:
    return float(np.std(values, ddof=1)) if len(values) >= MIN_SEGMENTS else None


def choose_rate(medians: Mapping[float, float | None], target: float = TARGET_SD) -> float:
    available = {rate: abs(value - target) for rate, value in medians.items() if value is not None}
    if not available:
        raise ValueError("no calibration standard deviations")
    distance = min(available.values())
    # 数学上等距时只消除浮点减法的舍入误差，不改变定标目标或候选频率。
    return min(
        rate
        for rate, gap in available.items()
        if gap == distance or math.isclose(gap, distance, rel_tol=1e-13)
    )


def t_coverage(x: float, dof: int) -> float:
    """整数自由度的双侧覆盖率；Abramowitz–Stegun 26.7.3、26.7.4。"""
    if dof < 1:
        raise ValueError("dof must be at least 1")
    theta = math.atan(abs(x) / math.sqrt(dof))
    sine, cosine = math.sin(theta), math.cos(theta)
    if dof % 2:
        series, term = 0.0, cosine
        for j in range(1, (dof - 1) // 2 + 1):
            series += term
            term *= (2 * j) / (2 * j + 1) * cosine**2
        return float(2 / math.pi * (theta + sine * series))
    series, term = 0.0, 1.0
    for j in range(dof // 2):
        series += term
        term *= (2 * j + 1) / (2 * j + 2) * cosine**2
    return float(sine * series)


def t_critical(confidence: float, dof: int) -> float:
    if not 0 < confidence < 1:
        raise ValueError("confidence must be between 0 and 1")
    low, high = 0.0, 1.0
    while t_coverage(high, dof) < confidence:
        high *= 2
    # 求根的相对数值精度，不是研究阈值。
    while high - low > high * 1e-13:
        middle = (low + high) / 2
        if t_coverage(middle, dof) < confidence:
            low = middle
        else:
            high = middle
    return (low + high) / 2


def t_bounds(values: Array, confidences: Sequence[float]) -> dict[str, list[float]]:
    centre = float(values.mean())
    scale = float(np.std(values, ddof=1)) / math.sqrt(len(values))
    out = {}
    for confidence in confidences:
        half = t_critical(confidence, len(values) - 1) * scale
        out[f"{confidence:g}"] = [centre - half, centre + half]
    return out


def boot_picks(count: int, draws: int, rng: np.random.Generator) -> Array:
    if count < 1 or draws < 1:
        raise ValueError("count and draws must be at least 1")
    return rng.integers(0, count, size=(draws, count))


def boot_bounds(
    values: Array, picks: Array, confidences: Sequence[float]
) -> dict[str, list[float]]:
    means = values[picks].mean(axis=1)
    out = {}
    for confidence in confidences:
        tail = (1 - confidence) / 2
        out[f"{confidence:g}"] = [float(x) for x in np.quantile(means, [tail, 1 - tail])]
    return out


def ruler_bounds(values: Array, picks: Array | None, confidences: Sequence[float]) -> Bounds | None:
    if len(values) < MIN_SEGMENTS or picks is None:
        return None
    return {
        "t_seg": t_bounds(values, confidences),
        "seg_boot": boot_bounds(values, picks, confidences),
    }


def calibration_history(
    frame: pd.DataFrame,
    found: Sequence[cp.Cycle],
    seed: int,
    index: int,
    *,
    months: int = MONTHS,
    rates: Sequence[float] = RATES,
) -> Record:
    rng = np.random.default_rng([seed, index, 1])
    made = cp.history(frame, found, rng, months)
    part = cp.market(made.frame, cp.TECH)
    level = cp.month_level(part.risky, part.month)
    bears, bulls = cp.slow_bears(level), long_bulls(level)
    uniforms = rng.random(len(level) - 1)
    sd = {}
    for rate in rates:
        targets = cp.plant_targets(bears, len(level), uniforms, rate)
        gross, held = cp.simulate(part.risky, part.cash, part.month, targets)
        reference, _ = cp.simulate(
            part.risky, part.cash, part.month, np.full(len(level), held.mean())
        )
        sd[rate_key(rate)] = sample_sd(
            simple_differences(np.log(gross), np.log(reference), part.month, bulls)
        )
    return {
        "history": index,
        "drawn": list(made.drawn),
        "slow_bears": len(bears),
        "long_bulls": len(bulls),
        "sd": sd,
        "error": None,
    }


def evaluate_history(
    frame: pd.DataFrame,
    found: Sequence[cp.Cycle],
    seed: int,
    index: int,
    *,
    rates: Sequence[float] = RATES,
    months: int = MONTHS,
    draws: int = DRAWS,
    confidences: Sequence[float] = cp.CONFIDENCES,
) -> Record:
    rng = np.random.default_rng([seed, index])
    made = cp.history(frame, found, rng, months)
    pooled: dict[str, dict[str, list[float]]] = {
        variant: {rate_key(rate): [] for rate in rates} for variant in cp.VARIANTS
    }
    bears_count: dict[str, int] = {}
    bulls_count: dict[str, int] = {}
    matched_exposure: dict[str, dict[str, float]] = {rate_key(rate): {} for rate in rates}
    for name in cp.MARKETS:
        part = cp.market(made.frame, name)
        if not len(part.rows):
            continue
        level = cp.month_level(part.risky, part.month)
        bears, bulls = cp.slow_bears(level), long_bulls(level)
        bears_count[name], bulls_count[name] = len(bears), len(bulls)
        uniforms = rng.random(len(level) - 1)
        variant = cp.TECH if name == cp.TECH else "three"
        for rate in rates:
            key = rate_key(rate)
            targets = cp.plant_targets(bears, len(level), uniforms, rate)
            gross, held = cp.simulate(part.risky, part.cash, part.month, targets)
            exposure = float(held.mean())
            matched_exposure[key][name] = exposure
            reference, _ = cp.simulate(
                part.risky, part.cash, part.month, np.full(len(level), exposure)
            )
            pooled[variant][key].extend(
                readings(np.log(gross) - np.log(reference), part.month, bulls).tolist()
            )
    counts = {variant: len(next(iter(pooled[variant].values()))) for variant in cp.VARIANTS}
    sampled = {
        variant: boot_picks(counts[variant], draws, rng)
        if counts[variant] >= MIN_SEGMENTS
        else None
        for variant in cp.VARIANTS
    }
    return {
        "history": index,
        "drawn": list(made.drawn),
        "days": len(made.frame),
        "slow_bears": bears_count,
        "long_bulls": bulls_count,
        "segments": counts,
        "matched_exposure": matched_exposure,
        "readings": pooled,
        "point": {
            variant: {key: float(np.mean(v)) if v else None for key, v in values.items()}
            for variant, values in pooled.items()
        },
        "bounds": {
            variant: {
                key: ruler_bounds(np.asarray(v), sampled[variant], confidences)
                for key, v in values.items()
            }
            for variant, values in pooled.items()
        },
        "error": None,
    }


def failed_history(index: int, error: str) -> Record:
    return {"history": index, "error": error}


def ruler_rows(
    records: Sequence[Record],
    variant: str,
    key: str,
    *,
    levels: Sequence[float] = LEVELS,
    confidences: Sequence[float] = cp.CONFIDENCES,
) -> Record:
    judged = [r for r in records if r["segments"][variant] >= MIN_SEGMENTS]
    points = np.asarray([r["point"][variant][key] for r in judged], dtype=float)
    centre = float(points.mean()) if len(points) else None
    rows = []
    total = len(records)
    for ruler in RULERS:
        for confidence in confidences:
            shares = {}
            zero_hits, line_hits = 0, 0
            for level in levels:
                shift = level - centre if centre is not None else 0.0
                intervals = [r["bounds"][variant][key][ruler][f"{confidence:g}"] for r in judged]
                hits = Counter(cp.judge(low + shift, high + shift) for low, high in intervals)
                shares[level_key(level)] = {
                    label: hits[label] / total if total else 0.0 for label in ("gain", "loss")
                }
                if level == 0:
                    zero_hits = hits["gain"] + hits["loss"]
                    line_hits = sum(low + shift > PRINT_LINE for low, _ in intervals)
            false_rate = zero_hits / total if total else 0.0
            upper = cp.wilson_upper(zero_hits, total) if total else None
            reverse = shares[level_key(POWER_LEVEL)]["gain"]
            rows.append(
                {
                    "ruler": ruler,
                    "confidence": confidence,
                    "shares": shares,
                    "false_rate": false_rate,
                    "wilson_upper": upper,
                    "reverse_rate": reverse,
                    "score": shares[level_key(POWER_LEVEL)]["loss"],
                    "line_share": line_hits / total if total else 0.0,
                    "eligible": total > 0
                    and false_rate <= cp.FALSE_LIMIT
                    and upper is not None
                    and upper <= cp.WILSON_LIMIT
                    and reverse <= cp.SIGN_LIMIT,
                }
            )
    return {"centre": centre, "spread": sample_sd(points), "judged": len(judged), "rows": rows}


def choose(rows: Sequence[Record]) -> Record:
    eligible = [row for row in rows if row["eligible"]]
    if not eligible:
        return {"verdict": "none_eligible", "ruler": None, "confidence": None, "score": None}
    best = max(eligible, key=lambda r: (r["score"], r["ruler"] == "t_seg", r["confidence"]))
    verdict = (
        "ready"
        if best["score"] >= cp.READY
        else "large_only"
        if best["score"] >= cp.LARGE
        else "below_50"
    )
    return {"verdict": verdict, **{k: best[k] for k in ("ruler", "confidence", "score")}}


def spread_of(values: Sequence[int]) -> Record:
    counts = Counter(values)
    return {
        "histories": len(values),
        "median": float(np.median(values)) if values else None,
        "min": min(values) if values else None,
        "max": max(values) if values else None,
        "counts": {str(k): counts[k] for k in sorted(counts)},
    }


def calibrate(records: Sequence[Record], rates: Sequence[float] = RATES) -> Record:
    table = {}
    medians: dict[float, float | None] = {}
    for rate in rates:
        key = rate_key(rate)
        values = [r["sd"][key] for r in records if r["sd"][key] is not None]
        medians[rate] = float(np.median(values)) if values else None
        table[key] = {
            "histories": len(values),
            "median": medians[rate],
            "q25": float(np.quantile(values, 0.25)) if values else None,
            "q75": float(np.quantile(values, 0.75)) if values else None,
        }
    return {
        "rates": table,
        "selected": choose_rate(medians) if any(v is not None for v in medians.values()) else None,
        "long_bulls": spread_of([r["long_bulls"] for r in records]),
        "slow_bears": spread_of([r["slow_bears"] for r in records]),
    }


def real_long_bulls(tech: pd.DataFrame, us: pd.DataFrame) -> dict[str, list[str]]:
    out = {}
    for name in (*cp.MARKETS, US):
        daily = us[cp.TECH] if name == US else tech[name]
        level = segments.monthly_level(daily.dropna())
        out[name] = [
            item.id for item in segments.long_bull_segments(level, segments.drop_segments(level))
        ]
    return out


def run_probe(
    tech: pd.DataFrame,
    us: pd.DataFrame,
    *,
    histories: int = HISTORIES,
    draws: int = DRAWS,
    seed: int = SEED,
    months: int = MONTHS,
    confidences: Sequence[float] = cp.CONFIDENCES,
    out_dir: Path | None = None,
    progress: Callable[[str, int, int], None] | None = None,
) -> Record:
    found, tail = cp.cycles(tech)
    us_found, us_tail = cp.cycles(us)
    enough = len(found) >= cp.MIN_CYCLES and len(us_found) >= cp.MIN_CYCLES
    calibration_records: list[Record] = []
    records: list[Record] = []
    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        for name in (CALIBRATION, JOURNAL):
            (out_dir / name).write_text("", encoding="utf-8")

    def collect(
        phase: str,
        filename: str,
        destination: list[Record],
        evaluate: Callable[[int], Record],
    ) -> None:
        for index in range(histories):
            try:
                record = evaluate(index)
            except Exception as exc:  # 一条出错也保留记录，并禁止完整判定。
                record = failed_history(index, f"{type(exc).__name__}: {exc}".splitlines()[0])
            destination.append(record)
            if out_dir is not None:
                with (out_dir / filename).open("a", encoding="utf-8") as sink:
                    sink.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            if progress is not None:
                progress(phase, index + 1, histories)

    calibration = None
    if enough:
        collect(
            "calibration",
            CALIBRATION,
            calibration_records,
            lambda i: calibration_history(us, us_found, seed, i, months=months),
        )
        calibration = calibrate([r for r in calibration_records if r["error"] is None])
        if calibration["selected"] is not None:
            collect(
                "histories",
                JOURNAL,
                records,
                lambda i: evaluate_history(
                    tech, found, seed, i, months=months, draws=draws, confidences=confidences
                ),
            )
    errors = {
        phase: [r for r in items if r["error"] is not None]
        for phase, items in (("calibration", calibration_records), ("histories", records))
    }
    good = [r for r in records if r["error"] is None]
    selected = calibration["selected"] if calibration is not None else None
    variants = {}
    for variant in cp.VARIANTS:
        tables = {
            rate_key(rate): ruler_rows(good, variant, rate_key(rate), confidences=confidences)
            for rate in RATES
        }
        read = (
            {"verdict": "incomplete", "ruler": None, "confidence": None, "score": None}
            if not enough or selected is None or any(errors.values()) or not good
            else choose(tables[rate_key(selected)]["rows"])
        )
        variants[variant] = {"reading": read, "tables": tables}
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": cp.SNAPSHOT_ID,
        "parameters": {
            "months": months,
            "histories": histories,
            "draws": draws,
            "seed": seed,
            "rates": list(RATES),
            "target_sd": TARGET_SD,
            "confidences": list(confidences),
            "levels": list(LEVELS),
        },
        "cycles": {cp.TECH: [asdict(c) for c in found], US: len(us_found)},
        "tail": {
            name: {"first": t[0], "last": t[-1], "months": len(t)} if t else None
            for name, t in ((cp.TECH, tail), (US, us_tail))
        },
        "real_long_bulls": real_long_bulls(tech, us),
        "calibration": calibration,
        "errors": errors,
        "long_bull_counts": {
            name: spread_of(
                [
                    r["segments"][name] if name == "three" else r["long_bulls"][name]
                    for r in good
                    if name == "three" or name in r["long_bulls"]
                ]
            )
            for name in (*cp.MARKETS, "three")
        },
        "slow_bear_counts": {
            name: spread_of([r["slow_bears"][name] for r in good if name in r["slow_bears"]])
            for name in cp.MARKETS
        },
        "variants": variants,
    }


VERDICT_TEXT = {
    "ready": "−2.5 检验力至少 80%，尺子可用；不代表真实规则有效或获准运行。",
    "large_only": "−2.5 检验力至少 50% 但不到 80%，只能量较大的代价。",
    "below_50": "−2.5 检验力不到 50%，量不清长牛代价。",
    "none_eligible": "没有合格的尺子，不能据此判长牛代价。",
    "incomplete": "周期不足、定标无结果、历史出错或没有成功历史；不作完整判定。",
}


def _pct(value: float | None) -> str:
    return "–" if value is None else f"{value:.1%}"


def _points(value: float | None) -> str:
    return "–" if value is None else f"{value * 100:+.2f}"


def _number(value: float | int | None) -> str:
    return "–" if value is None else f"{value:g}"


def _spread_lines(title: str, spreads: Mapping[str, Record]) -> list[str]:
    lines = [
        "",
        title,
        "",
        "| 市场或变体 | 历史条数 | 中位 | 最少 | 最多 | 段数：条数 |",
        "|---|---|---|---|---|---|",
    ]
    for name, spread in spreads.items():
        counts = "、".join(f"{k}：{v}" for k, v in spread["counts"].items()) or "–"
        lines.append(
            f"| {name} | {spread['histories']} | {_number(spread['median'])} | "
            f"{_number(spread['min'])} | {_number(spread['max'])} | {counts} |"
        )
    return lines


def render(result: Mapping[str, Any]) -> str:
    calibration = result["calibration"]
    selected = calibration["selected"] if calibration is not None else None
    lines = [
        f"# 科技行业长牛代价的检验力探针（{RUN_ID}）",
        "",
        f"- 依据：`{OWNER_DECISION}`（第 6 期章程第 14 节）。",
        f"- 数据：信号快照 `{cp.SNAPSHOT_ID}`，Hardw、Chips、Softw、Mkt-RF 和 RF，"
        f"只读到 {cp.LAST_DAY}。",
        "- 只用埋的规则；F0001、fast_slow 和别的已有配置都没有跑。不进台账、不算试验，k 不动。",
        "- 美股大盘只用来定标假退出的频率，不印、不判任何和代价大小有关的数。",
        "- 读数按日历年折，段间等权；一段之内简单年化差 = (1 + 基准年化) × (e^读数 − 1)。",
        "- 档位和「下界 > −1.67」都是对数点；QQQ 的 −1.67 换成对数点约 −1.45，这条线偏松。",
        "",
        "## 判定",
        "",
        f"选中的 f：{rate_key(selected) if selected is not None else '–'}（每年假退出的次数）。",
        "",
        "| 变体 | 判定 | 中文说明 | 尺子 / 置信度 | 分数 |",
        "|---|---|---|---|---|",
    ]
    for variant, item in result["variants"].items():
        read = item["reading"]
        ruler = "–" if read["ruler"] is None else f"{read['ruler']} / {read['confidence']:g}"
        lines.append(
            f"| {variant} | {read['verdict']} | {VERDICT_TEXT[read['verdict']]} | "
            f"{ruler} | {_pct(read['score'])} |"
        )
    lines += [
        "",
        "## 定标（美股大盘，只用来选 f）",
        "",
        "| f | 历史条数 | 标准差中位（点） | 四分位 25% | 四分位 75% | 选中 |",
        "|---|---|---|---|---|---|",
    ]
    for rate in result["parameters"]["rates"]:
        key = rate_key(rate)
        row = calibration["rates"][key] if calibration is not None else {}
        lines.append(
            f"| {key} | {row.get('histories', 0)} | {_points(row.get('median'))} | "
            f"{_points(row.get('q25'))} | {_points(row.get('q75'))} | "
            f"{'是' if rate == selected else '否'} |"
        )
    lines += [
        "",
        "## 周期和真实的长牛段（只印）",
        "",
        f"tech_ew 的周期 {len(result['cycles'][cp.TECH])} 个；"
        f"us_market 的周期 {result['cycles'][US]} 个；尾巴不抽。",
        "",
        "| 周期 | 起月 | 谷底月 | 高点月 | 跌幅 | 月数 |",
        "|---|---|---|---|---|---|",
    ]
    for cycle in result["cycles"][cp.TECH]:
        lines.append(
            f"| {cycle['id']} | {cycle['start']} | {cycle['end']} | {cycle['peak']} | "
            f"{_pct(cycle['change'])} | {cycle['months']} |"
        )
    lines.append("")
    for name, tail in result["tail"].items():
        description = f"{tail['first']}..{tail['last']}，{tail['months']} 个月" if tail else "无"
        lines.append(f"- {name} 尾巴（不抽）：{description}")
    for name, ids in result["real_long_bulls"].items():
        lines.append(f"- {name} 真实长牛段：{'、'.join(ids) or '–'}")
    if any(result["errors"].values()):
        lines += ["", "## 出错的历史", ""]
        for phase, errors in result["errors"].items():
            for error in errors:
                lines.append(f"- {phase} {error['history']}：{error['error']}")
    for variant, item in result["variants"].items():
        for key, table in item["tables"].items():
            role = "判的口径" if selected is not None and key == rate_key(selected) else "只印"
            lines += [
                "",
                f"## {variant} · f = {key}（{role}）",
                "",
                f"c₀ {_points(table['centre'])}；读数的标准差 {_points(table['spread'])}；"
                f"段 ≥ 2 的历史 {table['judged']} 条。",
                "",
                "| 做法 | 置信度 | 误判率 | Wilson 上限 | 反号率 | −2.5 检验力 | "
                "下界 > −1.67 | 合格 |",
                "|---|---|---|---|---|---|---|---|",
            ]
            for row in table["rows"]:
                shares = " | ".join(
                    _pct(row[k])
                    for k in ("false_rate", "wilson_upper", "reverse_rate", "score", "line_share")
                )
                lines.append(
                    f"| {row['ruler']} | {row['confidence']:g} | {shares} | "
                    f"{'是' if row['eligible'] else '否'} |"
                )
            levels = result["parameters"]["levels"]
            lines += [
                "",
                "各档判有收益 / 判有代价的占比：",
                "",
                "| 做法 | 置信度 | " + " | ".join(level_key(v) for v in levels) + " |",
                "|---|---|" + "---|" * len(levels),
            ]
            for row in table["rows"]:
                cells = " | ".join(
                    f"{_pct(row['shares'][level_key(v)]['gain'])} / "
                    f"{_pct(row['shares'][level_key(v)]['loss'])}"
                    for v in levels
                )
                lines.append(f"| {row['ruler']} | {row['confidence']:g} | {cells} |")
    lines += _spread_lines("## 每条历史的长牛段个数", result["long_bull_counts"])
    lines += _spread_lines(
        "## 每条历史重新划出的慢熊段数（和 TRADING-2565-61 同一批历史）",
        result["slow_bear_counts"],
    )
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
    """科技行业长牛代价的检验力探针，只用埋的规则。"""


@app.command("run")
def run_command(out: Annotated[Path, typer.Option(help="结果目录。")]) -> None:
    """先在美股大盘定标，再量科技行业长牛段尺子，不运行真实规则。"""
    try:
        tech, us = load_frames()
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"longbull_power: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None
    typer.echo(
        f"longbull_power: 数据已载入并截到 {cp.LAST_DAY}；"
        f"tech_ew {len(tech)} 天，周期 {len(cp.cycles(tech)[0])} 个；"
        f"us_market {len(us)} 天，周期 {len(cp.cycles(us)[0])} 个"
    )
    out.mkdir(parents=True, exist_ok=True)

    def progress(phase: str, done: int, total: int) -> None:
        every = 100 if phase == "calibration" else 25
        if done % every == 0 or done == total:
            typer.echo(f"longbull_power: {phase} {done}/{total}")

    result = run_probe(
        tech, us, histories=HISTORIES, draws=DRAWS, months=MONTHS, out_dir=out, progress=progress
    )
    write_result(result, out)
    calibration = result["calibration"]
    selected = calibration["selected"] if calibration is not None else None
    typer.echo(f"longbull_power: 选中的 f：{rate_key(selected) if selected is not None else '–'}")
    for variant, item in result["variants"].items():
        read = item["reading"]
        typer.echo(f"longbull_power: {variant} 判定 {read['verdict']}，分数 {_pct(read['score'])}")
    typer.echo(f"longbull_power: 写入 {out / SUMMARY} 和 {out / REPORT}")


if __name__ == "__main__":
    app()
