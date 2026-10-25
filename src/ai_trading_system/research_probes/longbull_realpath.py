"""埋的规则放到真实美股大盘路径上（TRADING-2565-68）。

依据 owner_decision:TRADING-2565:2026-10-25:longbull_realpath、第 6 期章程第 16 节。
只用 TRADING-2565-66 的两条腿埋规则，故意前视；不进任何账本、不算试验，k 不动。
F0001 和 fast_slow 等已有配置不在这里跑；三个导入的探针模块不改，只复用。
不抽合成历史，只用美股大盘那一条真实路径，只取 Mkt-RF 和 RF。
抽 500 组假退出，每组各个 R 共用随机数；选中的 R 按跑前写死的两条线判。
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import longbull_power as lp
from ai_trading_system.research_probes import longbull_reprobe as rp

# 第 6 期章程第 16 节，跑前固定的组数、选中 R 和两条判定线。
RUN_ID = "TRADING-2565-68"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-25:longbull_realpath"
SETS, SEED = 500, lp.SEED
LAGS, SELECTED = rp.LAGS, 3
QUIET, NOISY = 0.016, 0.019
SUMMARY, REPORT = "realpath.json", "realpath.md"
VERDICTS = ("path_quiet", "both", "plant_noisier", "incomplete")
# TRADING-2565-34 印的美股大盘 1929–2010 七个长牛段的起止月。
LONG_BULLS = (
    ("1933-03", "1937-02"),
    ("1942-05", "1946-05"),
    ("1947-06", "1961-12"),
    ("1962-07", "1968-11"),
    ("1974-10", "1987-08"),
    ("1987-12", "2000-03"),
    ("2002-10", "2007-10"),
)
# TRADING-2565-34 印的真实 F0001（fast_slow）在美股大盘 1929–2010 的两个平均敞口，只印
F0001_EXPOSURE, F0001_BULL_EXPOSURE = 0.656, 0.747
# 只消除浮点比较的舍入误差，不改变两条线
SLACK = 1e-12

Array = np.ndarray[Any, Any]
Record = dict[str, Any]
Spans = Sequence[tuple[int, int]]
Progress = Callable[[str, int, int], None]

app = typer.Typer()


@dataclass(frozen=True)
class RealPath:
    risky: Array
    cash: Array
    month: Array
    level: pd.Series
    bears: list[tuple[int, int]]
    bulls: list[tuple[int, int]]


def real_path(us: pd.DataFrame) -> RealPath:
    calendar = pd.DatetimeIndex(us.index).to_period("M")
    if calendar.empty or calendar[0] != cp.FIRST_MONTH:
        raise ValueError(f"the path must start in {cp.FIRST_MONTH}")
    frame = us.copy()
    frame["month"] = calendar.astype("int64").to_numpy(dtype=np.int64) - cp.FIRST_MONTH.ordinal
    part = cp.market(frame, cp.TECH)
    level = cp.month_level(part.risky, part.month)
    return RealPath(
        part.risky, part.cash, part.month, level, cp.slow_bears(level), lp.long_bulls(level)
    )


def span_months(level: pd.Series, spans: Spans) -> list[tuple[str, str]]:
    return [(str(level.index[first]), str(level.index[last])) for first, last in spans]


def uniforms_for(seed: int, index: int, count: int) -> Array:
    return np.random.default_rng([seed, index, 2]).random(count - 1)


def inside(month: Array, spans: Spans) -> Array:
    mask = np.zeros(len(month), dtype=bool)
    for first, last in spans:
        mask |= (month >= first) & (month <= last)
    return mask


def one_set(path: RealPath, lag: int, uniforms: Array, rate: float = rp.FALSE_RATE) -> Record:
    plant = rp.plant_legs(path.bears, len(path.level), uniforms, lag, rate)
    parts, exposure = rp.split(path.risky, path.cash, path.month, path.bulls, plant)
    _, held = cp.simulate(path.risky, path.cash, path.month, plant.targets)
    bull = inside(path.month, path.bulls)
    return {
        "simple": lp.sample_sd(parts.simple),
        "reading": lp.sample_sd(parts.reading),
        **{name: lp.sample_sd(parts.terms[name]) for name in rp.TERMS},
        "exposure": exposure,
        "bull_exposure": float(held[bull].mean()) if bull.any() else None,
        "segment_simple": [float(value) for value in parts.simple],
        "segment_exposure": [
            float(held[(path.month >= first) & (path.month <= last)].mean())
            for first, last in path.bulls
        ],
    }


def _present(values: Sequence[float | None]) -> list[float]:
    return [value for value in values if value is not None]


def _mean(values: Sequence[float | None]) -> float | None:
    present = _present(values)
    return float(np.mean(present)) if present else None


def _median(values: Sequence[float | None]) -> float | None:
    present = _present(values)
    return float(np.median(present)) if present else None


def summarize_lag(items: Sequence[Record]) -> Record:
    values = _present([item["simple"] for item in items])
    return {
        "sets": len(items),
        "median": _median(values),
        **{
            key: float(np.quantile(values, q)) if values else None
            for key, q in (("q25", 0.25), ("q75", 0.75), ("p05", 0.05), ("p95", 0.95))
        },
        "at_most_target": sum(value <= lp.TARGET_SD + SLACK for value in values),
        "at_most_band": sum(value <= lp.TARGET_SD + rp.BAND + SLACK for value in values),
        **{name: _median([item[name] for item in items]) for name in ("reading", *rp.TERMS)},
        "exposure": _mean([item["exposure"] for item in items]),
        "bull_exposure": _mean([item["bull_exposure"] for item in items]),
        "gap": _mean(
            [
                item["bull_exposure"] - item["exposure"]
                for item in items
                if item["bull_exposure"] is not None
            ]
        ),
    }


def segment_rows(path: RealPath, items: Sequence[Record]) -> list[Record]:
    excess = lp.simple_differences(
        np.log1p(path.risky), np.log1p(path.cash), path.month, path.bulls
    )
    whole = float(np.mean([item["exposure"] for item in items]))
    rows = []
    months = span_months(path.level, path.bulls)
    for i, (first, last) in enumerate(path.bulls):
        exposure = float(np.mean([item["segment_exposure"][i] for item in items]))
        rows.append(
            {
                "segment": i + 1,
                "start": months[i][0],
                "end": months[i][1],
                "months": last - first + 1,
                "excess": float(excess[i]),
                "exposure": exposure,
                "simple": float(np.mean([item["segment_simple"][i] for item in items])),
                "product": float((exposure - whole) * excess[i]),
            }
        )
    return rows


def verdict(item: Mapping[str, Any] | None, matches: bool) -> Record:
    median = item["median"] if item is not None else None
    key = (
        "incomplete"
        if not matches or median is None
        else "path_quiet"
        if median <= QUIET + SLACK
        else "plant_noisier"
        if median >= NOISY - SLACK
        else "both"
    )
    return {
        "key": key,
        "median": median,
        "factor": lp.TARGET_SD / median if median is not None and median != 0 else None,
    }


def parameters(sets: int, seed: int, lags: Sequence[int]) -> Record:
    return {
        "sets": sets,
        "seed": seed,
        "lags": list(lags),
        "selected": SELECTED,
        "false_rate": rp.FALSE_RATE,
        "fast_back": rp.FAST_BACK,
        "target_sd": lp.TARGET_SD,
        "band": rp.BAND,
        "quiet": QUIET,
        "noisy": NOISY,
    }


def probe(
    us: pd.DataFrame,
    *,
    sets: int | None = None,
    seed: int = SEED,
    lags: Sequence[int] = LAGS,
    expected: Sequence[tuple[str, str]] = LONG_BULLS,
    progress: Progress | None = None,
) -> Record:
    sets = SETS if sets is None else sets
    path = real_path(us)
    found = span_months(path.level, path.bulls)
    matches = found == list(expected)
    enough = len(path.bulls) >= lp.MIN_SEGMENTS
    collected: dict[str, list[Record]] = {lp.rate_key(lag): [] for lag in lags}
    if enough:
        for index in range(sets):
            uniforms = uniforms_for(seed, index, len(path.level))
            for lag in lags:
                collected[lp.rate_key(lag)].append(one_set(path, lag, uniforms))
            if progress is not None:
                progress("sets", index + 1, sets)
    no_false: dict[str, Record | None] = {}
    for lag in lags:
        plain = one_set(path, lag, np.ones(len(path.level) - 1), rate=0.0) if enough else None
        no_false[lp.rate_key(lag)] = (
            {
                **{name: plain[name] for name in ("simple", "exposure", "bull_exposure")},
                "gap": plain["bull_exposure"] - plain["exposure"]
                if plain["bull_exposure"] is not None
                else None,
            }
            if plain is not None
            else None
        )
    table = {key: summarize_lag(items) if items else None for key, items in collected.items()}
    chosen = collected.get(lp.rate_key(SELECTED), [])
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": cp.SNAPSHOT_ID,
        "parameters": parameters(sets, seed, lags),
        "path": {
            "months": len(path.level),
            "days": len(path.risky),
            "slow_bears": len(path.bears),
            "long_bulls": [list(span) for span in found],
            "matches": matches,
        },
        "f0001": {
            "sd": lp.TARGET_SD,
            "exposure": F0001_EXPOSURE,
            "bull_exposure": F0001_BULL_EXPOSURE,
        },
        "lags": table,
        "no_false": no_false,
        "segments": segment_rows(path, chosen) if chosen else [],
        "verdict": verdict(table.get(lp.rate_key(SELECTED)), matches),
    }


VERDICT_TEXT = {
    "path_quiet": "路径偏安静：埋的规则在这条路径上也只有 F0001 那么吵",
    "both": "两者都有：埋的规则比 F0001 吵，这条路径也偏安静",
    "plant_noisier": "埋的规则太吵：同一条路径上它比 F0001 吵得多",
    "incomplete": "没判：长牛段和 TRADING-2565-34 对不上，或者没有可算的组",
}


def _points(value: float | None, *, signed: bool = False) -> str:
    if value is None:
        return "–"
    return f"{value * 100:+.2f}" if signed else f"{value * 100:.2f}"


def _exposure(value: float | None) -> str:
    return "–" if value is None else f"{value:.3f}"


def _count(count: int, sets: int) -> str:
    return f"{count}（{count / sets:.1%}）" if sets else f"{count}（–）"


def _row(key: str, cells: Sequence[str]) -> str:
    return "| " + " | ".join((key, *cells)) + " |"


def _path_text(matches: bool) -> str:
    return "和 TRADING-2565-34 的七个长牛段" + ("逐段一致。" if matches else "对不上。")


def render(result: Mapping[str, Any]) -> str:
    params, path, read = result["parameters"], result["path"], result["verdict"]
    sets, selected = params["sets"], params["selected"]
    target = _points(params["target_sd"])
    band = _points(params["target_sd"] + params["band"])
    factor = "–" if read["factor"] is None else f"{read['factor']:.2f}"
    lines = [
        f"# 长牛代价：埋的规则放到真实美股大盘路径上（{result['run_id']}）",
        "",
        f"- 依据：`{result['owner_decision']}`，第 6 期章程第 16 节。",
        f"- 数据：信号快照 `{result['snapshot_id']}` 里 `{cp.FACTORS}` 的 Mkt-RF 和 RF，"
        f"切到 {cp.LAST_DAY}；只用美股大盘这一条真实路径，不抽合成历史，不用科技行业。",
        "- 只用 TRADING-2565-66 的两条腿埋规则；F0001、fast_slow 和别的已有配置都没有跑。"
        "不进台账、不算试验，k 不动。",
        f"- 快腿假退出的随机数抽 {sets} 组（种子 {params['seed']}），每组五个 R 共用；"
        f"q = {params['false_rate']:g}。",
        f"- 路径：{path['months']} 个月、{path['days']} 天，慢熊 {path['slow_bears']} 段，"
        f"长牛 {len(path['long_bulls'])} 段；{_path_text(path['matches'])}",
        "",
        "## 判定（跑之前写死）",
        "",
        f"| 选中的 R | 简单年化差段间标准差（{sets} 组的中位，点） | 线 | 判定 |",
        "|---|---|---|---|",
        f"| {selected} | {_points(read['median'])} | "
        f"≤ {_points(params['quiet'])} 路径偏安静；"
        f"≥ {_points(params['noisy'])} 埋的规则太吵；之间两者都有 | {read['key']} |",
        "",
        f"判定：{VERDICT_TEXT[read['key']]}。",
        "",
        f"真实 F0001 在同一条路径上是 {target}，比值 {target} ÷ 中位 = {factor}（只印）。",
        "",
        f"## 每个 R（点；敞口是 {sets} 组的平均）",
        "",
        "| R | 中位 | 下四分位 | 上四分位 | 5% | 95% | "
        f"≤ {target} | ≤ {band} | 精确读数（对数） | 再入滞后 | 假退出 | 满仓 | "
        "整条平均敞口 | 长牛平均敞口 | 差 |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for key, item in result["lags"].items():
        cells = (
            [
                *[_points(item[name]) for name in ("median", "q25", "q75", "p05", "p95")],
                *[_count(item[name], item["sets"]) for name in ("at_most_target", "at_most_band")],
                *[_points(item[name]) for name in ("reading", *rp.TERMS)],
                *[_exposure(item[name]) for name in ("exposure", "bull_exposure", "gap")],
            ]
            if item is not None
            else ["–"] * 14
        )
        lines.append(_row(key, cells))
    f0001 = result["f0001"]
    lines += [
        "",
        "真实 F0001（TRADING-2565-34 印的，只印）："
        f"整条平均敞口 {_exposure(f0001['exposure'])}，"
        f"长牛平均敞口 {_exposure(f0001['bull_exposure'])}，"
        f"差 {_exposure(f0001['bull_exposure'] - f0001['exposure'])}。",
        "",
        "## 没有假退出（q = 0，只印）",
        "",
        "| R | 简单年化差段间标准差 | 整条平均敞口 | 长牛平均敞口 | 差 |",
        "|---|---|---|---|---|",
    ]
    for key, item in result["no_false"].items():
        cells = (
            [
                _points(item["simple"]),
                *[_exposure(item[name]) for name in ("exposure", "bull_exposure", "gap")],
            ]
            if item is not None
            else ["–"] * 4
        )
        lines.append(_row(key, cells))
    lines += [
        "",
        f"## 选中的 R = {selected}：逐段（{sets} 组的平均，只印）",
        "",
        "一阶乘积 =（段内平均敞口 − 整条平均敞口）× 段的超额年化收益。",
        "",
        "| 段 | 首月 | 末月 | 月数 | 超额年化收益 | 段内平均敞口 | 简单年化差 | 一阶乘积 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in result["segments"]:
        lines.append(
            f"| {row['segment']} | {row['start']} | {row['end']} | {row['months']} | "
            f"{_points(row['excess'], signed=True)} | {_exposure(row['exposure'])} | "
            f"{_points(row['simple'], signed=True)} | {_points(row['product'], signed=True)} |"
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


def load_us(root: Path = PROJECT_ROOT) -> pd.DataFrame:
    snapshot = signal_sources.load_snapshot(root, cp.SNAPSHOT_ID)
    return lp.us_frame(snapshot.frames[cp.FACTORS])


def _progress(phase: str, done: int, total: int) -> None:
    if done % 100 == 0 or done == total:
        typer.echo(f"longbull_realpath: {phase} {done}/{total}")


@app.callback()
def main() -> None:
    """只把两条腿的埋规则放到真实美股大盘路径上，不运行真实规则。"""


@app.command("run")
def run_command(out: Annotated[Path, typer.Option(help="结果目录。")]) -> None:
    """固定真实路径，只抽假退出的随机数，按两条线判。"""
    try:
        us = load_us()
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"longbull_realpath: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None
    typer.echo(f"longbull_realpath: 数据已载入并截到 {cp.LAST_DAY}；us_market {len(us)} 天")
    result = probe(us, sets=SETS, progress=_progress)
    write_result(result, out)
    matches = result["path"]["matches"]
    typer.echo(
        "longbull_realpath: 长牛段和 TRADING-2565-34 " + ("逐段一致" if matches else "对不上")
    )
    read = result["verdict"]
    typer.echo(
        f"longbull_realpath: R = {SELECTED} 中位 {_points(read['median'])} 点，判定 {read['key']}"
    )
    typer.echo(f"longbull_realpath: 写入 {out / SUMMARY} 和 {out / REPORT}")


if __name__ == "__main__":
    app()
