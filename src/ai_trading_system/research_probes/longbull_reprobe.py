"""长牛代价重测（TRADING-2565-66）。

依据 owner_decision:TRADING-2565:2026-10-25:longbull_reprobe、第 6 期章程第 15 节。
只用埋的规则，故意前视；不进任何账本、不算试验，k 不动。
F0001 和 fast_slow 等已有配置不在这里跑；cycle_power 和 longbull_power 不改，只导入。
两条腿各管一半敞口，在慢熊峰后第 2 个月退出；快腿在谷后第 2 个月回来，
慢腿在谷后第 R + 1 个月回来。快腿其余月份以固定概率假退出一个月，不看收益。
decompose 只在美股大盘的合成历史上拆成再入滞后、假退出、满仓三项，核三件事。
run 重做拆解，三件都成立才量科技行业的尺子；读数、尺子和读法沿用前次探针。
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import longbull_power as lp

# 第 6 期章程第 15 节及跑前规格第 0、2–4 节，固定的埋规则和拆解读法。
RUN_ID = "TRADING-2565-66"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-25:longbull_reprobe"
MONTHS, HISTORIES, DRAWS, SEED = lp.MONTHS, lp.HISTORIES, lp.DRAWS, lp.SEED
LAGS = (0, 3, 6, 9, 12)
FALSE_RATE, FAST_BACK, HALF = 0.35, 2, 0.5
TERMS = ("reentry", "false", "full")
MOVE, BAND, IDENTITY = 0.003, 0.0036, 1e-9
FIRST = 0
DECOMPOSE, DECOMPOSE_SUMMARY, DECOMPOSE_REPORT = "decompose.jsonl", "decompose.json", "decompose.md"
CALIBRATION_KEYS = ("history", "drawn", "slow_bears", "long_bulls", "sd", "error")
# 只消除浮点减法的舍入误差，不改变 (a)、(b) 的线
SLACK = 1e-12

Array = np.ndarray[Any, Any]
Record = dict[str, Any]
Progress = Callable[[str, int, int], None]

app = typer.Typer()


@dataclass(frozen=True)
class Plant:
    targets: Array
    lag: Array
    false: Array


def plant_legs(
    bears: Sequence[tuple[int, int]],
    count: int,
    uniforms: Array,
    lag: int,
    rate: float = FALSE_RATE,
) -> Plant:
    if len(uniforms) != count - 1:
        raise ValueError("uniforms must have count - 1 entries")
    if lag < 0:
        raise ValueError("lag must not be negative")
    fast, slow = np.ones(count, dtype=bool), np.ones(count, dtype=bool)
    reentry, false = np.zeros(count, dtype=bool), np.zeros(count, dtype=bool)
    for peak, trough in bears:
        first = peak + cp.EXIT_MONTHS + 1
        fast[first : trough + FAST_BACK] = False
        slow[first : trough + lag + 1] = False
        reentry[trough + 1 : trough + max(FAST_BACK - 1, lag) + 1] = True
    false[1:] = (uniforms < rate) & fast[1:]
    fast[false] = False
    return Plant(HALF * fast + HALF * slow, reentry, false)


def day_terms(plant: Plant, month: Array) -> Array:
    return np.asarray(np.where(plant.lag[month], 0, np.where(plant.false[month], 1, 2)))


@dataclass(frozen=True)
class Parts:
    spans: Sequence[tuple[int, int]]
    terms: dict[str, Array]
    reading: Array
    first_order: Array
    simple: Array


def split(
    risky: Array, cash: Array, month: Array, spans: Sequence[tuple[int, int]], plant: Plant
) -> tuple[Parts, float]:
    gross, held = cp.simulate(risky, cash, month, plant.targets)
    exposure = float(held.mean())
    reference, base = cp.simulate(risky, cash, month, np.full(len(plant.targets), exposure))
    own, benchmark = np.log(gross), np.log(reference)
    difference = own - benchmark
    kind = day_terms(plant, month)
    return (
        Parts(
            spans,
            {
                name: lp.readings(np.where(kind == i, difference, 0.0), month, spans)
                for i, name in enumerate(TERMS)
            },
            lp.readings(difference, month, spans),
            lp.readings((held - base) * (risky - cash), month, spans),
            lp.simple_differences(own, benchmark, month, spans),
        ),
        exposure,
    )


@dataclass(frozen=True)
class UsParts:
    drawn: tuple[str, ...]
    slow_bears: int
    parts: dict[str, Parts]


def us_parts(
    frame: pd.DataFrame,
    found: Sequence[cp.Cycle],
    seed: int,
    index: int,
    *,
    months: int = MONTHS,
    lags: Sequence[int] = LAGS,
) -> UsParts:
    rng = np.random.default_rng([seed, index, 1])
    made = cp.history(frame, found, rng, months)
    part = cp.market(made.frame, cp.TECH)
    level = cp.month_level(part.risky, part.month)
    bears, bulls = cp.slow_bears(level), lp.long_bulls(level)
    uniforms = rng.random(len(level) - 1)
    return UsParts(
        made.drawn,
        len(bears),
        {
            lp.rate_key(lag): split(
                part.risky,
                part.cash,
                part.month,
                bulls,
                plant_legs(bears, len(level), uniforms, lag),
            )[0]
            for lag in lags
        },
    )


def us_history(
    frame: pd.DataFrame,
    found: Sequence[cp.Cycle],
    seed: int,
    index: int,
    *,
    months: int = MONTHS,
    lags: Sequence[int] = LAGS,
) -> Record:
    made = us_parts(frame, found, seed, index, months=months, lags=lags)
    items = {}
    for key, part in made.parts.items():
        residual = sum(part.terms.values()) - part.reading
        items[key] = {
            **{name: lp.sample_sd(values) for name, values in part.terms.items()},
            "reading": lp.sample_sd(part.reading),
            "simple": lp.sample_sd(part.simple),
            "gap": float(np.median(np.abs(part.first_order - part.reading)))
            if len(part.spans)
            else None,
            "identity": float(np.max(np.abs(residual))) if len(part.spans) else 0.0,
        }
    return {
        "history": index,
        "drawn": list(made.drawn),
        "slow_bears": made.slow_bears,
        "long_bulls": len(next(iter(made.parts.values())).spans),
        "sd": {key: item["simple"] for key, item in items.items()},
        "parts": items,
        "error": None,
    }


def first_rows(
    frame: pd.DataFrame,
    found: Sequence[cp.Cycle],
    seed: int,
    *,
    months: int = MONTHS,
    lags: Sequence[int] = LAGS,
) -> list[Record]:
    made = us_parts(frame, found, seed, FIRST, months=months, lags=lags)
    rows = []
    for key, part in made.parts.items():
        for i, (first, last) in enumerate(part.spans):
            terms = {name: float(values[i]) for name, values in part.terms.items()}
            rows.append(
                {
                    "lag": key,
                    "segment": i + 1,
                    "first": first,
                    "last": last,
                    "years": (last - first + 1) / 12,
                    **terms,
                    "sum": sum(terms.values()),
                    "reading": float(part.reading[i]),
                    "first_order": float(part.first_order[i]),
                    "gap": float(part.first_order[i] - part.reading[i]),
                }
            )
    return rows


def check(
    table: Mapping[str, Record],
    selected: int | None,
    records: Sequence[Record],
    lags: Sequence[int] = LAGS,
) -> Record:
    term, spread = None, None
    if selected is not None and all(
        table[lp.rate_key(lag)][name] is not None for lag in lags for name in TERMS
    ):
        chosen = table[lp.rate_key(selected)]
        term = max(TERMS, key=lambda name: chosen[name])
        values = [table[lp.rate_key(lag)][term] for lag in lags]
        spread = max(values) - min(values)
    near = [
        lp.rate_key(lag)
        for lag in lags
        if table[lp.rate_key(lag)]["simple"] is not None
        and abs(table[lp.rate_key(lag)]["simple"] - lp.TARGET_SD) <= BAND + SLACK
    ]
    residual = max(
        (record["parts"][lp.rate_key(lag)]["identity"] for record in records for lag in lags),
        default=None,
    )
    return {
        "a": {
            "term": term,
            "spread": spread,
            "holds": spread is not None and spread >= MOVE - SLACK,
        },
        "b": {"lags": near, "holds": bool(near)},
        "c": {"residual": residual, "holds": residual is not None and residual <= IDENTITY},
    }


def _median(values: Sequence[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return float(np.median(present)) if present else None


def summarize(records: Sequence[Record], rows: list[Record], lags: Sequence[int] = LAGS) -> Record:
    calibration = lp.calibrate(
        [{key: r[key] for key in CALIBRATION_KEYS} for r in records], rates=lags
    )
    table = {}
    judged = [r for r in records if r["long_bulls"] >= lp.MIN_SEGMENTS]
    for lag in lags:
        key = lp.rate_key(lag)
        table[key] = {
            "histories": len(judged),
            **{
                name: _median([r["parts"][key][name] for r in judged])
                for name in (*TERMS, "reading", "simple")
            },
            "gap": _median([r["parts"][key]["gap"] for r in records]),
        }
    checks = check(table, calibration["selected"], records, lags)
    return {
        "histories": len(records),
        "lags": table,
        "selected": calibration["selected"],
        "checks": checks,
        "all_hold": all(item["holds"] for item in checks.values()),
        "first_history": rows,
    }


def evaluate_history(
    frame: pd.DataFrame,
    found: Sequence[cp.Cycle],
    seed: int,
    index: int,
    *,
    lags: Sequence[int] = LAGS,
    months: int = MONTHS,
    draws: int = DRAWS,
    confidences: Sequence[float] = cp.CONFIDENCES,
) -> Record:
    rng = np.random.default_rng([seed, index])
    made = cp.history(frame, found, rng, months)
    pooled: dict[str, dict[str, list[float]]] = {
        variant: {lp.rate_key(lag): [] for lag in lags} for variant in cp.VARIANTS
    }
    bears_count: dict[str, int] = {}
    bulls_count: dict[str, int] = {}
    matched_exposure: dict[str, dict[str, float]] = {lp.rate_key(lag): {} for lag in lags}
    for name in cp.MARKETS:
        part = cp.market(made.frame, name)
        if not len(part.rows):
            continue
        level = cp.month_level(part.risky, part.month)
        bears, bulls = cp.slow_bears(level), lp.long_bulls(level)
        bears_count[name], bulls_count[name] = len(bears), len(bulls)
        uniforms = rng.random(len(level) - 1)
        variant = cp.TECH if name == cp.TECH else "three"
        for lag in lags:
            key = lp.rate_key(lag)
            targets = plant_legs(bears, len(level), uniforms, lag).targets
            gross, held = cp.simulate(part.risky, part.cash, part.month, targets)
            exposure = float(held.mean())
            matched_exposure[key][name] = exposure
            reference, _ = cp.simulate(
                part.risky, part.cash, part.month, np.full(len(level), exposure)
            )
            pooled[variant][key].extend(
                lp.readings(np.log(gross) - np.log(reference), part.month, bulls).tolist()
            )
    counts = {variant: len(next(iter(pooled[variant].values()))) for variant in cp.VARIANTS}
    sampled = {
        variant: lp.boot_picks(counts[variant], draws, rng)
        if counts[variant] >= lp.MIN_SEGMENTS
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
                key: lp.ruler_bounds(np.asarray(v), sampled[variant], confidences)
                for key, v in values.items()
            }
            for variant, values in pooled.items()
        },
        "error": None,
    }


def failed_history(index: int, error: str) -> Record:
    return lp.failed_history(index, error)


def same_draws(ours: Mapping[str, Any], theirs: Mapping[str, Any]) -> bool:
    return all(ours[key] == theirs[key] for key in ("drawn", "slow_bears", "long_bulls"))


def _append(out_dir: Path | None, filename: str, record: Record) -> None:
    if out_dir is not None:
        with (out_dir / filename).open("a", encoding="utf-8") as sink:
            sink.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def _prepare(out_dir: Path | None, filenames: Sequence[str]) -> None:
    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        for filename in filenames:
            (out_dir / filename).write_text("", encoding="utf-8")


def _us_pass(
    us: pd.DataFrame,
    found: Sequence[cp.Cycle],
    histories: int,
    seed: int,
    months: int,
    out_dir: Path | None,
    progress: Progress | None,
    *,
    compare: bool,
) -> tuple[list[Record], list[bool]]:
    records: list[Record] = []
    comparisons = []
    for index in range(histories):
        try:
            record = us_history(us, found, seed, index, months=months)
            if compare and record["error"] is None:
                previous = lp.calibration_history(
                    us, found, seed, index, months=months, rates=(1.0,)
                )
                comparisons.append(same_draws(record, previous))
        except Exception as exc:  # 保留失败并继续；任一失败阻止第 2 步。
            record = failed_history(index, f"{type(exc).__name__}: {exc}".splitlines()[0])
        records.append(record)
        decomposed = (
            {key: record[key] for key in ("history", "drawn", "long_bulls", "parts", "error")}
            if record["error"] is None
            else record
        )
        _append(out_dir, DECOMPOSE, decomposed)
        if compare:
            _append(
                out_dir,
                lp.CALIBRATION,
                {key: record[key] for key in CALIBRATION_KEYS}
                if record["error"] is None
                else record,
            )
        if progress is not None:
            progress("calibration", index + 1, histories)
    return records, comparisons


def decomposition(
    us: pd.DataFrame, found: Sequence[cp.Cycle], records: Sequence[Record], seed: int, months: int
) -> Record | None:
    good = [r for r in records if r["error"] is None]
    if len(found) < cp.MIN_CYCLES or not good or len(good) != len(records):
        return None
    return summarize(good, first_rows(us, found, seed, months=months))


def parameters(
    histories: int,
    draws: int | None,
    seed: int,
    months: int,
    confidences: Sequence[float] | None,
) -> Record:
    result: Record = {
        "months": months,
        "histories": histories,
        "seed": seed,
        "lags": list(LAGS),
        "false_rate": FALSE_RATE,
        "fast_back": FAST_BACK,
        "target_sd": lp.TARGET_SD,
        "band": BAND,
        "move": MOVE,
    }
    if draws is not None and confidences is not None:
        result.update(draws=draws, confidences=list(confidences), levels=list(lp.LEVELS))
    return result


def _errors(records: Sequence[Record]) -> list[Record]:
    return [
        {"history": r["history"], "error": r["error"]} for r in records if r["error"] is not None
    ]


def decompose_probe(
    us: pd.DataFrame,
    *,
    histories: int = HISTORIES,
    seed: int = SEED,
    months: int = MONTHS,
    out_dir: Path | None = None,
    progress: Progress | None = None,
) -> Record:
    found, _ = cp.cycles(us)
    _prepare(out_dir, (DECOMPOSE,))
    records = (
        _us_pass(us, found, histories, seed, months, out_dir, progress, compare=False)[0]
        if len(found) >= cp.MIN_CYCLES
        else []
    )
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": cp.SNAPSHOT_ID,
        "step": "decompose",
        "parameters": parameters(histories, None, seed, months, None),
        "cycles": len(found),
        "errors": _errors(records),
        "decompose": decomposition(us, found, records, seed, months),
    }


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
    progress: Progress | None = None,
) -> Record:
    found, tail = cp.cycles(tech)
    us_found, us_tail = cp.cycles(us)
    enough = len(found) >= cp.MIN_CYCLES and len(us_found) >= cp.MIN_CYCLES
    _prepare(out_dir, (lp.CALIBRATION, DECOMPOSE, lp.JOURNAL))
    calibration_records: list[Record] = []
    records: list[Record] = []
    us_same: list[bool] = []
    tech_same: list[bool] = []
    calibration, decomposed = None, None
    if enough:
        calibration_records, us_same = _us_pass(
            us, us_found, histories, seed, months, out_dir, progress, compare=True
        )
        calibration = lp.calibrate(
            [
                {key: r[key] for key in CALIBRATION_KEYS}
                for r in calibration_records
                if r["error"] is None
            ],
            rates=LAGS,
        )
        decomposed = decomposition(us, us_found, calibration_records, seed, months)
        if (
            calibration["selected"] is not None
            and decomposed is not None
            and decomposed["all_hold"]
        ):
            for index in range(histories):
                try:
                    record = evaluate_history(
                        tech,
                        found,
                        seed,
                        index,
                        months=months,
                        draws=draws,
                        confidences=confidences,
                    )
                    if record["error"] is None:
                        previous = lp.evaluate_history(
                            tech,
                            found,
                            seed,
                            index,
                            months=months,
                            draws=draws,
                            rates=(1.0,),
                            confidences=confidences[:1],
                        )
                        tech_same.append(same_draws(record, previous))
                except Exception as exc:  # 保留失败并继续；任一失败禁止完整判定。
                    record = failed_history(index, f"{type(exc).__name__}: {exc}".splitlines()[0])
                records.append(record)
                _append(out_dir, lp.JOURNAL, record)
                if progress is not None:
                    progress("histories", index + 1, histories)
    errors = {"calibration": _errors(calibration_records), "histories": _errors(records)}
    good = [r for r in records if r["error"] is None]
    selected = calibration["selected"] if calibration is not None else None
    complete = (
        enough
        and selected is not None
        and decomposed is not None
        and decomposed["all_hold"]
        and not any(errors.values())
        and bool(good)
    )
    variants = {}
    for variant in cp.VARIANTS:
        tables = {
            lp.rate_key(lag): lp.ruler_rows(
                good, variant, lp.rate_key(lag), confidences=confidences
            )
            for lag in LAGS
        }
        read = (
            lp.choose(tables[lp.rate_key(selected)]["rows"])
            if complete and selected is not None
            else {"verdict": "incomplete", "ruler": None, "confidence": None, "score": None}
        )
        variants[variant] = {"reading": read, "tables": tables}
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": cp.SNAPSHOT_ID,
        "step": "run",
        "parameters": parameters(histories, draws, seed, months, confidences),
        "cycles": {cp.TECH: [asdict(c) for c in found], lp.US: len(us_found)},
        "tail": {
            name: {"first": t[0], "last": t[-1], "months": len(t)} if t else None
            for name, t in ((cp.TECH, tail), (lp.US, us_tail))
        },
        "real_long_bulls": lp.real_long_bulls(tech, us),
        "calibration": calibration,
        "decompose": decomposed,
        "errors": errors,
        "same_as_previous": {
            "calibration": all(us_same) if us_same else None,
            "histories": all(tech_same) if tech_same else None,
        },
        "long_bull_counts": {
            name: lp.spread_of(
                [
                    r["segments"][name] if name == "three" else r["long_bulls"][name]
                    for r in good
                    if name == "three" or name in r["long_bulls"]
                ]
            )
            for name in (*cp.MARKETS, "three")
        },
        "slow_bear_counts": {
            name: lp.spread_of([r["slow_bears"][name] for r in good if name in r["slow_bears"]])
            for name in cp.MARKETS
        },
        "variants": variants,
    }


def _points(value: float | None, *, signed: bool = False) -> str:
    if value is None:
        return "–"
    return f"{value * 100:+.2f}" if signed else f"{value * 100:.2f}"


def _pct(value: float | None) -> str:
    return "–" if value is None else f"{value:.1%}"


def _number(value: float | int | None) -> str:
    return "–" if value is None else f"{value:g}"


def _lag_row(key: str, row: Mapping[str, Any], selected: int | None) -> str:
    values = " | ".join(_points(row[name]) for name in (*TERMS, "reading", "simple", "gap"))
    chosen = selected is not None and key == lp.rate_key(selected)
    return f"| {key} | {row['histories']} | {values} | {'是' if chosen else '否'} |"


def decompose_lines(item: Mapping[str, Any] | None) -> list[str]:
    if item is None:
        return ["拆解没有结果（周期不足或历史出错），不能跑第 2 步。"]
    a, b, c = (item["checks"][key] for key in ("a", "b", "c"))

    def holds(flag: bool) -> str:
        return "成立" if flag else "不成立"

    lines = [
        "### 读法（跑之前写死）",
        "",
        "| 项 | 读法 | 结果 |",
        "|---|---|---|",
        "| (a) | 选中的 R 上段间标准差中位最大的那一项，五个 R 里最大减最小 ≥ 0.30 点 | "
        f"{holds(a['holds'])}（{a['term'] or '–'}，{_points(a['spread'])} 点） |",
        "| (b) | 至少一个 R 的简单年化差段间标准差中位在 1.24 ± 0.36 点里 | "
        f"{holds(b['holds'])}（R = {'、'.join(b['lags']) or '–'}） |",
        "| (c) | 三项之和等于精确读数，最大残差 ≤ 1e-9 | "
        f"{holds(c['holds'])}（{_number(c['residual'])}） |",
        "",
        "三件都成立：可以跑第 2 步（run）。"
        if item["all_hold"]
        else "有一件不成立：停下，不跑第 2 步，回来改规格。",
        "",
        f"### 每个 R 的段间标准差中位（{item['histories']} 条历史，点）",
        "",
        "| R | 历史条数 | 再入滞后 | 假退出 | 满仓 | 精确读数 | 简单年化差 | 近似差 | 选中 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    lines += [_lag_row(key, row, item["selected"]) for key, row in item["lags"].items()]
    lines += [
        "",
        "### 第 0 条历史的逐段拆解（埋的规则，点/年）",
        "",
        "| R | 段 | 首月 | 末月 | 年数 | 再入滞后 | 假退出 | 满仓 | 三项之和 | 精确读数 | "
        "一阶近似 | 近似 − 精确 |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in item["first_history"]:
        values = " | ".join(
            _points(row[name], signed=True)
            for name in (*TERMS, "sum", "reading", "first_order", "gap")
        )
        lines.append(
            f"| {row['lag']} | {row['segment']} | {row['first']} | {row['last']} | "
            f"{row['years']:.2f} | {values} |"
        )
    return lines


def _intro(*, decompose: bool) -> list[str]:
    title = "第 1 步拆解" if decompose else "两条腿的埋规则"
    data = (
        f"- 数据：信号快照 `{cp.SNAPSHOT_ID}` 的 Mkt-RF 和 RF，只读到 {cp.LAST_DAY}；"
        "只用美股大盘的合成历史，不读科技行业。"
        if decompose
        else f"- 数据：信号快照 `{cp.SNAPSHOT_ID}`，Hardw、Chips、Softw、Mkt-RF 和 RF，"
        f"只读到 {cp.LAST_DAY}。"
    )
    return [
        f"# 长牛代价重测：{title}（{RUN_ID}）",
        "",
        f"- 依据：`{OWNER_DECISION}`（第 6 期章程第 15 节）。",
        data,
        "- 只用埋的规则；F0001、fast_slow 和别的已有配置都没有跑。不进台账、不算试验，k 不动。",
        "- 埋的规则：两条腿各管一半敞口；慢熊里两条腿都在峰后第 2 个月退出，"
        "快腿在谷底后第 2 个月回来，慢腿在谷底后第 R + 1 个月回来；其余月份快腿每月以概率 "
        "q = 0.35 假退出一个月。腿的状态只看峰、谷和随机数，不看收益。",
        "- 三项按日分：再入窗口里的日子归再入滞后，否则假退出的日子归假退出，其余归满仓；"
        "每项都是日对数差按长牛段的日历年数折年，三项之和等于精确读数。",
        "- 一阶近似只印：(策略持有敞口 − 基准持有敞口) × (风险收益 − 现金收益)，"
        "它和精确读数有二阶项的差，不用来判定。",
        "- 第 0 条历史的逐段数是埋的规则在一条合成历史上的数，不是任何真实规则的代价。",
    ]


def render_decompose(result: Mapping[str, Any]) -> str:
    lines = _intro(decompose=True)
    lines += ["", f"美股大盘的周期 {result['cycles']} 个；尾巴不抽。", ""]
    if result["errors"]:
        lines += ["## 出错的历史", ""]
        lines += [f"- {r['history']}：{r['error']}" for r in result["errors"]]
        lines.append("")
    lines += decompose_lines(result["decompose"])
    lines += ["", "production_effect=none；broker_action=none。", ""]
    return "\n".join(lines)


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


def _ruler_lines(
    variant: str, key: str, table: Record, role: str, levels: Sequence[float]
) -> list[str]:
    lines = [
        "",
        f"## {variant} · R = {key}（{role}）",
        "",
        f"c₀ {_points(table['centre'], signed=True)}；读数的标准差 {_points(table['spread'])}；"
        f"段 ≥ 2 的历史 {table['judged']} 条。",
        "",
        "| 做法 | 置信度 | 误判率 | Wilson 上限 | 反号率 | −2.5 检验力 | 下界 > −1.67 | 合格 |",
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
    lines += [
        "",
        "各档判有收益 / 判有代价的占比：",
        "",
        "| 做法 | 置信度 | " + " | ".join(lp.level_key(v) for v in levels) + " |",
        "|---|---|" + "---|" * len(levels),
    ]
    for row in table["rows"]:
        cells = " | ".join(
            f"{_pct(row['shares'][lp.level_key(v)]['gain'])} / "
            f"{_pct(row['shares'][lp.level_key(v)]['loss'])}"
            for v in levels
        )
        lines.append(f"| {row['ruler']} | {row['confidence']:g} | {cells} |")
    return lines


def render(result: Mapping[str, Any]) -> str:
    calibration = result["calibration"]
    selected = calibration["selected"] if calibration is not None else None
    selected_key = lp.rate_key(selected) if selected is not None else "–"
    lines = _intro(decompose=False)
    lines += [
        "- 读数、档位、两把尺子和读法都照 TRADING-2565-63；档位和「下界 > −1.67」都是对数点。",
        "",
        "## 判定",
        "",
        f"选中的 R：{selected_key}（慢腿在谷底后第 R + 1 个月回来）；q = 0.35。",
        "",
    ]
    if result["decompose"] is None or not result["decompose"]["all_hold"]:
        lines += ["拆解的三件没有都成立，第 2 步没有跑科技行业。", ""]
    lines += [
        "| 变体 | 判定 | 中文说明 | 尺子 / 置信度 | 分数 |",
        "|---|---|---|---|---|",
    ]
    for variant, item in result["variants"].items():
        read = item["reading"]
        ruler = "–" if read["ruler"] is None else f"{read['ruler']} / {read['confidence']:g}"
        description = (
            "周期不足、定标或拆解无结果、三件未全成立、历史出错或没有成功历史；不作完整判定。"
            if read["verdict"] == "incomplete"
            else lp.VERDICT_TEXT[read["verdict"]]
        )
        lines.append(
            f"| {variant} | {read['verdict']} | {description} | {ruler} | {_pct(read['score'])} |"
        )
    lines += [
        "",
        "## 定标（美股大盘，只用来选 R）",
        "",
        "| R | 历史条数 | 标准差中位（点） | 四分位 25% | 四分位 75% | 选中 |",
        "|---|---|---|---|---|---|",
    ]
    for lag in result["parameters"]["lags"]:
        key = lp.rate_key(lag)
        row = calibration["rates"][key] if calibration is not None else {}
        lines.append(
            f"| {key} | {row.get('histories', 0)} | {_points(row.get('median'))} | "
            f"{_points(row.get('q25'))} | {_points(row.get('q75'))} | "
            f"{'是' if lag == selected else '否'} |"
        )
    decomposed = result["decompose"]
    if decomposed is not None and selected is not None:
        row = decomposed["lags"][selected_key]
        values = "；".join(
            f"{name} {_points(row[name])}" for name in (*TERMS, "reading", "simple", "gap")
        )
        lines += ["", f"选中的 R 在拆解表里的那一行：R = {selected_key}；{values}（点）。"]
    lines += ["", "## 和 TRADING-2565-63 同一批历史（只印）", ""]
    for label, phase in (("美股大盘", "calibration"), ("科技行业", "histories")):
        same = result["same_as_previous"][phase]
        shown = "–（没有比）" if same is None else str(same)
        lines.append(f"- {label}：抽到的周期、慢熊段数、长牛段数逐条相同：{shown}")
    lines += [
        "",
        "## 周期和真实的长牛段（只印）",
        "",
        f"tech_ew 的周期 {len(result['cycles'][cp.TECH])} 个；"
        f"us_market 的周期 {result['cycles'][lp.US]} 个；尾巴不抽。",
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
            lines += [f"- {phase} {e['history']}：{e['error']}" for e in errors]
    for variant, item in result["variants"].items():
        for key, table in item["tables"].items():
            role = "判的口径" if selected is not None and key == selected_key else "只印"
            lines += _ruler_lines(variant, key, table, role, result["parameters"]["levels"])
    lines += _spread_lines("## 每条历史的长牛段个数", result["long_bull_counts"])
    lines += _spread_lines(
        "## 每条历史重新划出的慢熊段数（和 TRADING-2565-61、-63 同一批历史）",
        result["slow_bear_counts"],
    )
    lines += ["", "## 附录：第 1 步的拆解", ""]
    lines += decompose_lines(decomposed)
    lines += ["", "production_effect=none；broker_action=none。", ""]
    return "\n".join(lines)


def _write(result: Mapping[str, Any], out_dir: Path, summary: str, report: str, text: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / summary).write_text(
        json.dumps(result, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / report).write_text(text, encoding="utf-8")


def write_decompose(result: Mapping[str, Any], out_dir: Path) -> None:
    _write(result, out_dir, DECOMPOSE_SUMMARY, DECOMPOSE_REPORT, render_decompose(result))


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    _write(result, out_dir, lp.SUMMARY, lp.REPORT, render(result))


def load_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    return lp.load_frames()


def _load() -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        tech, us = load_frames()
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"longbull_reprobe: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None
    typer.echo(
        f"longbull_reprobe: 数据已载入并截到 {cp.LAST_DAY}；"
        f"tech_ew {len(tech)} 天，周期 {len(cp.cycles(tech)[0])} 个；"
        f"us_market {len(us)} 天，周期 {len(cp.cycles(us)[0])} 个"
    )
    return tech, us


def _progress(phase: str, done: int, total: int) -> None:
    every = 100 if phase == "calibration" else 25
    if done % every == 0 or done == total:
        typer.echo(f"longbull_reprobe: {phase} {done}/{total}")


@app.callback()
def main() -> None:
    """长牛代价重测，只用两条腿的埋规则，先拆解再量尺子。"""


@app.command("decompose")
def decompose_command(out: Annotated[Path, typer.Option(help="结果目录。")]) -> None:
    """只拆美股大盘合成历史，三件都成立才可跑第 2 步。"""
    _, us = _load()
    result = decompose_probe(
        us, histories=HISTORIES, months=MONTHS, out_dir=out, progress=_progress
    )
    write_decompose(result, out)
    item = result["decompose"]
    holds = item is not None and item["all_hold"]
    typer.echo(f"longbull_reprobe: {'拆解三件都成立' if holds else '拆解有一件不成立'}")
    typer.echo(f"longbull_reprobe: 写入 {out / DECOMPOSE_SUMMARY} 和 {out / DECOMPOSE_REPORT}")


@app.command("run")
def run_command(out: Annotated[Path, typer.Option(help="结果目录。")]) -> None:
    """重核拆解，三件都成立才在科技行业的合成历史上量尺子。"""
    tech, us = _load()
    result = run_probe(
        tech, us, histories=HISTORIES, draws=DRAWS, months=MONTHS, out_dir=out, progress=_progress
    )
    write_result(result, out)
    calibration = result["calibration"]
    selected = calibration["selected"] if calibration is not None else None
    typer.echo(
        f"longbull_reprobe: 选中的 R：{lp.rate_key(selected) if selected is not None else '–'}"
    )
    for variant, item in result["variants"].items():
        read = item["reading"]
        typer.echo(
            f"longbull_reprobe: {variant} 判定 {read['verdict']}，分数 {_pct(read['score'])}"
        )
    typer.echo(f"longbull_reprobe: 写入 {out / lp.SUMMARY} 和 {out / lp.REPORT}")


if __name__ == "__main__":
    app()
