"""第 5 期章程第 3.2 节只印的状态表，只用到 2010-12。

快照 sig-ed9e988ffe7c 的 shiller_price、shiller_earnings、shiller_cpi、
baa_monthly、aaa_monthly 五个源经 monthly_known_at 工作日对齐，取月底已知值。
分位至少 120 个有值月份，没定义记没有。不判、不计、不开题，不读验证窗口，
不写研究账本，不改门，不授权生产或券商动作。
"""

from __future__ import annotations

import json
import statistics
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger, trial
from ai_trading_system.research_bench.settings import EpisodeSpec, LoadedSettings, load_settings
from ai_trading_system.research_bench.sources import SourceSpec, monthly_known_at
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_probes import conditional_reserve, gate_power
from ai_trading_system.research_probes import policy_table as pt

RUN_ID = "TRADING-2565-56"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-23:research_loop_p5"
PLAN_ID = "T0008"
SNAPSHOT_ID = "sig-ed9e988ffe7c"
FIRST_MONTH = "1871-01"
LAST_MONTH = "2010-12"
PRICE = "shiller_price"
EARNINGS = "shiller_earnings"
CPI = "shiller_cpi"
BAA = "baa_monthly"
AAA = "aaa_monthly"
# TRADING-2565-37 登记的发布滞后（月）和已知滞后（交易日）；快照规格不同就停下。
LAGS = {PRICE: (0, 0), EARNINGS: (15, 0), CPI: (15, 0), BAA: (0, 5), AAA: (0, 5)}
# 章程第 3.2 节的口径和跑前的线：只印，不是研究接受门槛。
AVERAGE_MONTHS = 120
MIN_HISTORY = 120
PERCENTILE = 0.8
CHANGE_MONTHS = 6
BEFORE_MONTHS = 6
SEARCH_MONTHS = 24
LINE_SLOW = 8
LINE_BULL = 0.5
SHORT_MONTHS = 6
SHARE_START = "1929-01"
STATES = {
    "cape": "估值：CAPE 分位 ≥ 0.8",
    "spread": "利差水平：Baa − Aaa 分位 ≥ 0.8",
    "widening": "利差变化：Baa − Aaa 6 个月变化 > 0",
}
MARKETS: tuple[trial.Market, ...] = ("us_market", "qqq")
MARKET_TEXT = pt.MARKET_TEXT
RUN_JSON = "state_table.json"
RUN_MD = "state_table.md"
MARKS = {1.0: "●", 0.0: "○"}
UNDEFINED = "–"
app = typer.Typer(help="第 5 期状态表：只印，不判、不计、不开题。")


def months() -> pd.PeriodIndex:
    return pd.period_range(FIRST_MONTH, LAST_MONTH, freq="M")


def monthly_frame(frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    columns = {}
    for source in LAGS:
        if source not in frames:
            raise ValueError(f"snapshot has no {source}")
        frame = frames[source]
        if "value" not in frame:
            raise ValueError(f"{source} frame needs a value column")
        index = pd.DatetimeIndex(frame.index).to_period("M")
        kept = index <= pd.Period(LAST_MONTH, "M")
        index = index[kept]
        if not index.is_unique:
            raise ValueError(f"{source} repeats a month")
        values = pd.to_numeric(frame.loc[kept, "value"], errors="coerce")
        columns[source] = pd.Series(values.to_numpy(), index=index).reindex(months())
    return pd.DataFrame(columns, index=months())


def known_month_end(values: pd.Series, release: int, lag: int) -> pd.Series:
    index = pd.PeriodIndex(values.index, freq="M")
    calendar = pd.bdate_range(index[0].start_time, index[-1].end_time)
    daily = monthly_known_at(
        pd.Series(values.to_numpy(), index=index.to_timestamp(how="start")),
        calendar,
        release,
        lag,
    )
    ends = daily.groupby(calendar.to_period("M")).tail(1)
    return pd.Series(ends.to_numpy(), index=pd.DatetimeIndex(ends.index).to_period("M")).reindex(
        index
    )


def known_offsets(release: int, lag: int) -> list[int]:
    values = pd.Series(np.arange(len(months()), dtype=float), index=months())
    known = known_month_end(values, release, lag)
    return sorted({int(v) for v in (values - known).dropna()})


def expanding_percentile(values: pd.Series, min_history: int = MIN_HISTORY) -> pd.Series:
    history: list[float] = []
    out = []
    for value in values.to_numpy(dtype=float):
        if pd.isna(value):
            out.append(np.nan)
            continue
        history.append(float(value))
        out.append(
            sum(v <= value for v in history) / len(history)
            if len(history) >= min_history
            else np.nan
        )
    return pd.Series(out, index=values.index)


def state_frame(frame: pd.DataFrame, lags: Mapping[str, tuple[int, int]] = LAGS) -> pd.DataFrame:
    if lags[EARNINGS] != lags[CPI]:
        raise ValueError("earnings and CPI need the same publication lags")
    real = frame[EARNINGS] / frame[CPI]
    average = real.rolling(AVERAGE_MONTHS, min_periods=AVERAGE_MONTHS).mean()
    cape = (
        known_month_end(frame[PRICE], *lags[PRICE])
        / known_month_end(frame[CPI], *lags[CPI])
        / known_month_end(average, *lags[EARNINGS])
    )
    spread = known_month_end(frame[BAA], *lags[BAA]) - known_month_end(frame[AAA], *lags[AAA])
    change = spread - spread.shift(CHANGE_MONTHS)
    cape_pct, spread_pct = expanding_percentile(cape), expanding_percentile(spread)
    return pd.DataFrame(
        {
            "cape_value": cape,
            "cape_pct": cape_pct,
            "spread_value": spread,
            "spread_pct": spread_pct,
            "spread_change": change,
            "cape": (cape_pct >= PERCENTILE).astype(float).where(cape_pct.notna()),
            "spread": (spread_pct >= PERCENTILE).astype(float).where(spread_pct.notna()),
            "widening": (change > 0).astype(float).where(change.notna()),
        },
        index=frame.index,
    )


def marks(state: pd.Series, first: pd.Period, last: pd.Period) -> str:
    return "".join(
        MARKS.get(v, UNDEFINED) for v in state.reindex(pd.period_range(first, last, freq="M"))
    )


def nearest_switch(state: pd.Series, start: pd.Period, end: pd.Period) -> int | None:
    candidates = [
        int(month.ordinal - start.ordinal)
        for month in pd.period_range(start - SEARCH_MONTHS, end, freq="M")
        if month in state.index
        and month - 1 in state.index
        and state.get(month) == 1.0
        and state.get(month - 1) != 1.0
    ]
    return min(candidates, key=lambda d: (abs(d), d)) if candidates else None


def _span(spec: EpisodeSpec) -> tuple[pd.Period, pd.Period, dict[str, Any]]:
    start, end = pd.Period(spec.start, "M"), pd.Period(spec.end, "M")
    return (
        start,
        end,
        {
            "id": spec.id,
            "start": spec.start,
            "end": spec.end,
            "months": int(end.ordinal - start.ordinal) + 1,
        },
    )


def slow_rows(states: pd.DataFrame, specs: Sequence[EpisodeSpec]) -> list[dict[str, Any]]:
    rows = []
    for spec in specs:
        start, end, row = _span(spec)
        for name in STATES:
            state = states[name]
            before = marks(state, start - (BEFORE_MONTHS - 1), start)
            inside = marks(state, start, end)
            row[name] = {
                "before": before,
                "before_on": MARKS[1.0] in before,
                "inside": inside,
                "inside_on": inside.count(MARKS[1.0]),
                "start_on": bool(state.get(start, np.nan) == 1.0),
                "switch": nearest_switch(state, start, end),
            }
        rows.append(row)
    return rows


def bull_rows(states: pd.DataFrame, specs: Sequence[EpisodeSpec]) -> dict[str, Any]:
    rows = []
    for spec in specs:
        start, end, row = _span(spec)
        for name in STATES:
            on = marks(states[name], start, end).count(MARKS[1.0])
            row[name] = {"on": on, "share": on / row["months"]}
        rows.append(row)
    total = sum(r["months"] for r in rows)
    pooled: dict[str, Any] = {"months": total}
    for name in STATES:
        on = sum(r[name]["on"] for r in rows)
        pooled[name] = {"on": on, "share": on / total if total else None}
    return {"rows": rows, "pooled": pooled}


def line(slow: Sequence[Mapping[str, Any]], bull: Mapping[str, Any]) -> dict[str, Any]:
    found = {}
    for name in STATES:
        on = sum(bool(r[name]["before_on"]) for r in slow)
        share = bull["pooled"][name]["share"]
        found[name] = {
            "slow_on": on,
            "slow_count": len(slow),
            "bull_share": share,
            "passes": on >= LINE_SLOW and share is not None and share < LINE_BULL,
        }
    return found


def decline_rows(
    states: pd.DataFrame, specs: Sequence[EpisodeSpec], excluded: Sequence[str]
) -> dict[str, Any]:
    rows = []
    state = states["widening"]
    for spec in specs:
        start, end, row = _span(spec)
        row.update(
            counted=spec.id not in excluded,
            inside_on=marks(state, start, end).count(MARKS[1.0]),
            start_on=bool(state.get(start, np.nan) == 1.0),
            switch=nearest_switch(state, start, end),
        )
        rows.append(row)
    counted = [r["months"] for r in rows if r["counted"]]
    return {
        "rows": rows,
        "counted": len(counted),
        "short": sum(n <= SHORT_MONTHS for n in counted),
        "median_months": statistics.median(counted) if counted else None,
    }


def _bounds(values: pd.Series) -> tuple[str | None, str | None, int]:
    first, last = values.first_valid_index(), values.last_valid_index()
    return (
        None if first is None else str(first),
        None if last is None else str(last),
        0 if first is None else int(values.loc[first:last].isna().sum()),
    )


def series_rows(
    frame: pd.DataFrame, states: pd.DataFrame, lags: Mapping[str, tuple[int, int]] = LAGS
) -> dict[str, Any]:
    sources = {}
    for source, (release, lag) in lags.items():
        first, last, missing = _bounds(frame[source])
        sources[source] = {
            "release_lag_months": release,
            "known_lag_trading_days": lag,
            "first_month": first,
            "last_month": last,
            "missing_inside": missing,
            "offsets": known_offsets(release, lag),
        }
    found = {}
    for name, column in (
        ("cape", "cape_value"),
        ("spread", "spread_value"),
        ("widening", "spread_change"),
    ):
        found[name] = {
            "first_value": _bounds(states[column])[0],
            "first_defined": _bounds(states[name])[0],
            "share_on": float(
                (
                    states[name].reindex(pd.period_range(SHARE_START, LAST_MONTH, freq="M")) == 1.0
                ).mean()
            ),
        }
    return {"sources": sources, "states": found}


def build(
    frame: pd.DataFrame,
    gate: trial.TrialGate,
    loaded: LoadedSettings,
    lags: Mapping[str, tuple[int, int]] = LAGS,
) -> dict[str, Any]:
    states = state_frame(frame, lags)
    markets = {}
    for market in MARKETS:
        window = pt.market_window(loaded, gate, market)
        slow, bull = gate.markets[market].segment_lists(window.window_id)
        row: dict[str, Any] = {
            "window_id": window.window_id,
            "slow_bear": slow_rows(states, slow),
            "long_bull": bull_rows(states, bull),
        }
        if market == "us_market":
            row["line"] = line(row["slow_bear"], row["long_bull"])
            row["declines"] = decline_rows(
                states,
                gate.markets[market].decline_list(window.window_id),
                conditional_reserve.T0008_RECORD[market]["excluded"],
            )
        markets[market] = row
    return {"series": series_rows(frame, states, lags), "markets": markets}


def run_table(
    frame: pd.DataFrame,
    gate: trial.TrialGate,
    loaded: LoadedSettings,
    root_dir: Path,
    *,
    provenance: Mapping[str, Any],
    lags: Mapping[str, tuple[int, int]] = LAGS,
) -> dict[str, Any]:
    return dict(
        gate_power._clean(
            {
                "run_id": RUN_ID,
                "owner_decision": OWNER_DECISION,
                "snapshot_id": SNAPSHOT_ID,
                "months": {"first": FIRST_MONTH, "last": LAST_MONTH},
                "gate_sha256": ledger.chain_digest(
                    (root_dir / trial.TRIALS_DIR / "prereg.yaml").read_bytes()
                ),
                "provenance": dict(provenance),
                **build(frame, gate, loaded, lags),
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    )


def _table(
    lines: list[str], title: str, headers: Sequence[str], rows: Sequence[Sequence[Any]]
) -> None:
    lines.extend(["", title, "", pt._table_row(headers), "|" + "---|" * len(headers)])
    lines.extend(pt._table_row(row) for row in rows)


def _offsets(offsets: Sequence[int]) -> str:
    return "、".join(f"M−{offset}" for offset in offsets)


def _share(value: Any) -> str:
    return "—" if value is None else f"{value:.2f}"


def _yes(value: Any) -> str:
    return "是" if value else "否"


def _switch(value: Any) -> str:
    return "—" if value is None else f"{value:+d}"


def render(result: Mapping[str, Any]) -> str:
    series = result["series"]
    lines = [
        "# 第 5 期状态表（只印，不判、不计、不开题）",
        "",
        f"- 运行 {result['run_id']}；依据 {result['owner_decision']}（第 5 期章程第 3.2 节）；"
        "B 区，只读，不改门，不写研究账本，不读验证窗口。",
        f"- 提交 {result['provenance']['commit']}；快照 {result['snapshot_id']}；"
        f"月份 {result['months']['first']}..{result['months']['last']}；"
        f"门文件 sha256 {result['gate_sha256']}。",
        "- 口径：评估台 monthly_known_at，工作日日历，月 M 的状态取月 M 最后一个工作日已知的值；"
        "CAPE = 价 / 最新已知 CPI / 10 年平均实际盈利，盈利、CPI 晚 15 个月，"
        "最近 15 个月的通胀没折进价里；分位 = 到当月为止（含当月）有值月份里 ≤ 当月值的比例，"
        "从第 120 个有值的月份起有定义；没定义记「没有」，表里印 –，●为真、○为假。",
    ]
    _table(
        lines,
        "## 序列和滞后",
        ["源", "发布滞后（月）", "已知滞后（交易日）", "有值的月份", "中间缺月", "月底用的是"],
        [
            [
                s,
                r["release_lag_months"],
                r["known_lag_trading_days"],
                f"{r['first_month']}..{r['last_month']}" if r["first_month"] else "没有值",
                r["missing_inside"],
                _offsets(r["offsets"]),
            ]
            for s, r in series["sources"].items()
        ],
    )
    _table(
        lines,
        "## 三个状态",
        ["状态", "第一个有值的月份", "第一个有定义的月份", "1929–2010 为真的月份占比"],
        [
            [STATES[n], r["first_value"], r["first_defined"], _share(r["share_on"])]
            for n, r in series["states"].items()
        ],
    )
    lines.append(
        "- 分位含当月（当月是到当月为止的最大值时为 1.0），第 120 个有值的月份起有定义；"
        "CAPE 的价按评估台对齐是 M−1 的月均价，比章程「当月月均价」晚一个月，方向保守；"
        "价折实用的是 M−16 的 CPI，最近 15 个月的通胀没折进去："
        "通胀高的年份 CAPE 偏高，通缩年份偏低。"
    )
    us = result["markets"]["us_market"]
    _table(
        lines,
        "## 跑前写下的线（第 5 期章程第 3.2 节，只给续期卡用，不判）",
        ["状态", "起点前 6 个月内为真的慢熊段", "长牛段合计为真的占比", "线：≥ 8 段且 < 0.5"],
        [
            [
                STATES[n],
                f"{r['slow_on']}/{r['slow_count']}",
                _share(r["bull_share"]),
                "过线" if r["passes"] else "不过线",
            ]
            for n, r in us["line"].items()
        ],
    )
    for market in MARKETS:
        found = result["markets"][market]
        for name in STATES:
            _table(
                lines,
                f"## {MARKET_TEXT[market]} 慢熊段：{STATES[name]}",
                [
                    "段",
                    "起止",
                    "起点前 6 个月",
                    "有没有",
                    "段内逐月",
                    "段内为真/月数",
                    "起点为真",
                    "翻的滞后（月）",
                ],
                [
                    [
                        r["id"],
                        f"{r['start']}..{r['end']}",
                        r[name]["before"],
                        "有" if r[name]["before_on"] else "没有",
                        r[name]["inside"],
                        f"{r[name]['inside_on']}/{r['months']}",
                        _yes(r[name]["start_on"]),
                        _switch(r[name]["switch"]),
                    ]
                    for r in found["slow_bear"]
                ],
            )
        bulls = found["long_bull"]
        rows = [
            [r["id"], *[f"{r[n]['on']}（{_share(r[n]['share'])}）" for n in STATES]]
            for r in bulls["rows"]
        ]
        if market == "us_market":
            rows.append(
                [
                    "合计",
                    *[
                        f"{bulls['pooled'][n]['on']}（{_share(bulls['pooled'][n]['share'])}）"
                        for n in STATES
                    ],
                ]
            )
        _table(
            lines,
            f"## {MARKET_TEXT[market]}：长牛段里状态为真的月数（占比）",
            ["段", *STATES.values()],
            rows,
        )
    declines = us["declines"]
    _table(
        lines,
        "## 利差 6 个月变化对组合尺子的下跌 10% 段（教训 2）",
        ["段", "起止", "月数", "T0008 计入", "段内为真/月数", "起点为真", "翻的滞后（月）"],
        [
            [
                r["id"],
                f"{r['start']}..{r['end']}",
                r["months"],
                _yes(r["counted"]),
                f"{r['inside_on']}/{r['months']}",
                _yes(r["start_on"]),
                _switch(r["switch"]),
            ]
            for r in declines["rows"]
        ],
    )
    median = declines["median_months"]
    lines.append(
        f"- 计入的 {declines['counted']} 段里月数 ≤ 6 的 {declines['short']} 段，"
        f"月数中位 {median if median is not None else '—'}。"
    )
    return "\n".join([*lines, "", "production_effect=none；broker_action=none。", ""])


def write_json(result: Mapping[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(gate_power._clean(result), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def load_inputs() -> tuple[
    pd.DataFrame, dict[str, SourceSpec], trial.TrialGate, LoadedSettings, Path
]:
    loaded = load_settings()
    root_dir = ledger.research_root(loaded)
    gate, plan = trial.load_trial(root_dir, PLAN_ID)
    problems = trial.gate_problems(gate, loaded)
    if plan.data.signal_snapshot_id != SNAPSHOT_ID:
        problems.append(f"signal snapshot must be {SNAPSHOT_ID}")
    if problems:
        raise ValueError("; ".join(problems))
    snapshot = signal_sources.load_snapshot(PROJECT_ROOT, SNAPSHOT_ID)
    specs = {spec.source_id: spec for spec in snapshot.specs}
    return monthly_frame(snapshot.frames), specs, gate, loaded, root_dir


def check_problems(
    frame: pd.DataFrame,
    specs: Mapping[str, SourceSpec],
    gate: trial.TrialGate,
    loaded: LoadedSettings,
) -> list[str]:
    problems = []
    for source, (release, lag) in LAGS.items():
        if source not in specs:
            problems.append(f"{source}: 快照里没有")
            continue
        spec = specs[source]
        r, k = spec.release_lag_months, spec.known_lag_trading_days
        if (r, k) != (release, lag):
            problems.append(f"{source}: 滞后 {r}/{k}，应是 {release}/{lag}")
        values = frame[source].loc[: pd.Period(LAST_MONTH, "M")]
        first, last, missing = _bounds(values)
        if first is None:
            problems.append(f"{source}: 2010-12 以前没有值")
            continue
        if missing:
            problems.append(f"{source}: 中间缺月")
        if last != LAST_MONTH:
            problems.append(f"{source}: 只到 {last}")
    for market in MARKETS:
        window = pt.market_window(loaded, gate, market)
        slow, bull = gate.markets[market].segment_lists(window.window_id)
        if not slow or not bull:
            problems.append(f"{market}: 门里没有慢熊段或长牛段")
        if market == "us_market" and not gate.markets[market].decline_list(window.window_id):
            problems.append("us_market: 门里没有下跌段")
    return problems


def _load() -> tuple[pd.DataFrame, dict[str, SourceSpec], trial.TrialGate, LoadedSettings, Path]:
    try:
        return load_inputs()
    except (OSError, ValueError, KeyError, trial.TrialError) as exc:
        typer.echo(f"state_table: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None


def _print_checks(
    frame: pd.DataFrame,
    specs: Mapping[str, SourceSpec],
    gate: trial.TrialGate,
    loaded: LoadedSettings,
) -> bool:
    for source, (release, lag) in LAGS.items():
        first, last, _ = _bounds(frame[source])
        span = f"{first}..{last}" if first else "没有值"
        typer.echo(
            f"state_table: {source} {span}；月底用的是 {_offsets(known_offsets(release, lag))}"
        )
    problems = check_problems(frame, specs, gate, loaded)
    for problem in problems:
        typer.echo(f"state_table: 不行：{problem}")
    typer.echo("state_table: 开跑前检查没过" if problems else "state_table: 可以")
    return not problems


@app.callback()
def main() -> None:
    """只印第 5 期状态表，不判、不计、不开题。"""


@app.command("check")
def check_command() -> None:
    frame, specs, gate, loaded, _ = _load()
    if not _print_checks(frame, specs, gate, loaded):
        raise typer.Exit(code=1)


@app.command("run")
def run_command(out: Annotated[Path, typer.Option()]) -> None:
    if any((out / name).exists() for name in (RUN_JSON, RUN_MD)):
        typer.echo("state_table: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    frame, specs, gate, loaded, root_dir = _load()
    if not _print_checks(frame, specs, gate, loaded):
        raise typer.Exit(code=1)
    result = run_table(frame, gate, loaded, root_dir, provenance=git_provenance(PROJECT_ROOT))
    out.mkdir(parents=True, exist_ok=True)
    write_json(result, out / RUN_JSON)
    (out / RUN_MD).write_text(render(result), encoding="utf-8")
    for name, r in result["markets"]["us_market"]["line"].items():
        typer.echo(
            f"state_table: {STATES[name]} 慢熊段起点前为真 {r['slow_on']}/{r['slow_count']}，"
            f"长牛为真占比 {_share(r['bull_share'])}，{'过线' if r['passes'] else '不过线'}"
        )
    typer.echo(f"state_table: 完成；结果目录 {out}")


if __name__ == "__main__":
    app()
