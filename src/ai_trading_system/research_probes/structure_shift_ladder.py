"""The structure-shift strategy with a scaled buy-back (TRADING-2565-126).

Under owner_decision:TRADING-2565:2026-11-04:scaled_entry_ladder,
reprint TRADING-2565-122 on QQQ 1999–2010 and the registered
qqq_after_2010_owner_print window, checking every printed total return.
Append a scaled buy-back to both its original X2 row and its below-line X2
row: after each sale, buy with 10%, 20%, 30%, and 40% of the sale cash at
closes at or below 0.9, 0.8, 0.7, and 0.6 times the sale close. After the
first tranche, a close back at the sale price buys the rest. The first
entry or structure-shift signal after the sale buys the rest at any time.
Read only QQQ. Print only: no verdicts, ledger entries, gate or ruler changes,
daily reports or broker actions.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import typer

from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_exit_combo as cb
from ai_trading_system.research_probes import entry_structure_shift as es
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import structure_shift_periods as sp
from ai_trading_system.research_probes import structure_shift_rebuy as rb

RUN_ID = "TRADING-2565-126"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-04:scaled_entry_ladder"
SPANS = ("dev", "qqq")
# Baker's tranches: (multiple of the sale close, share of the cash at sale).
LEVELS = ((0.9, 0.1), (0.8, 0.2), (0.7, 0.3), (0.6, 0.4))
LADDER = {"x2_s5_ladder": "x2_s5", "x2_s5_under_ladder": "x2_s5_under"}
SHOWN = ("x2_s5", "x2_s5_ladder", "x2_s5_under", "x2_s5_under_ladder", "hold")
TRIPS_SHOWN = ("x2_s5", "x2_s5_under")
STRATEGY_TEXT = {
    **sp.STRATEGY_TEXT,
    "x2_s5_ladder": "X2 回落 8%，分批买回，加入场和结构转变买回（左右各 5 天）",
    "x2_s5_under_ladder": "X2 回落 8%（只在 200 日线下算），分批买回，"
    "加入场和结构转变买回（左右各 5 天）",
}
HOW_TEXT = {**sp.HOW_TEXT, "ladder": "四档买满"}
FULL_TEXT = {
    "ladder": "四档买满",
    "recovery": "回到卖价",
    "entry": "入场信号",
    "shift": "结构转变",
    "none": "没买满",
}
CHECK_JSON = "structure_shift_ladder_check.json"
RESULT_JSON = "structure_shift_ladder.json"
RESULT_MD = "structure_shift_ladder.md"
RULE_TEXT = (
    "只印不判，不检验、不下结论，不是交易建议。两段都已看过 TRADING-2565-122 的结果；"
    "档位和比例是 Baker 定的，没到 0.9X 前靠入场或结构转变买回、按收盘价成交，"
    "是看过 2000–2010 的探索以后定的。先重算核对 TRADING-2565-120 和 TRADING-2565-122，"
    "满仓起点、交易日数和十四行总收益都等于它们印的才印新行。只读 QQQ，"
    "纳斯达克 100 这次不读。结果只作描述，真检验只有前瞻记录。"
)
NOTE = (
    "在 X2 原规则和只在 200 日线下算的两行上各加一行分批买回。每次卖出（X2 和 L 止损都算）"
    "以后，卖出那个收盘记作 X，卖出后的现金记作 C；从卖出的下一个收盘起，"
    "收盘不高于 0.9X、0.8X、0.7X、0.6X 时，各用 C 的 10%、20%、30%、40% 在那个收盘买，"
    "每档只买一次，一个收盘可以成交几档。0.9X 那档成交以后，第一个收盘不低于 X "
    "就把剩下的现金全买。卖出以后第一个 v0_close 入场或结构转变入场（同一个收盘算 v0_close），"
    "不管成交过几档，都在它的收盘把剩下的现金全买；靠结构转变买满的带那次的 L 止损。"
    "同一个收盘先算档，再依次算四档买满、回到 X、v0_close 和结构转变。"
    "买满以后 X2 从买满那个收盘起重新记最高收盘；没买满之前不卖。"
    "每次成交按成交金额扣 0.05%，现金 0 收益。在场时间是有持仓的交易日占比，"
    "平均仓位是每个交易日开始时持仓占净值的比例的平均。"
    "平均买回差价是买满时的平均买价 ÷ X − 1（没买满的，剩下的现金按最后一个收盘算）。"
    "离 200 日线 = 出场响那个收盘 ÷ 当天的 200 日线 − 1。"
)
app = typer.Typer(help="结构转变策略的分批买回：只印不判。")


class Ladder(NamedTuple):
    """Closing equity, transaction path, and mean opening position weight."""

    values: tuple[float, ...]
    route: cb.Route
    exposure: float | None


def signal_after(
    close: Sequence[float], entries: Sequence[int], moves: Sequence[es.Shift], v: int
) -> tuple[int, str, float | None] | None:
    """Choose the first signal strictly after the sale, preferring entry on ties."""
    entry = next((e for e in entries if e > v), None)
    move = next((m for m in moves if m.entry > v), None)
    if entry is not None and (move is None or entry <= move.entry):
        return entry, "entry", None
    if move is not None:
        return move.entry, "shift", close[move.low]
    return None


def walk_ladder(
    data: xc.Prepared,
    entries: Sequence[int],
    moves: Sequence[es.Shift],
    s0: int,
    fire: sp.Fire,
) -> Ladder:
    """Walk sale cycles with partial purchases and no exits until fully invested."""
    close = data.close
    n = len(close)
    shares = (1.0 - cb.COST) / close[s0]
    cash = 0.0
    values = [shares * close[s0]]
    held = [False] * n
    buys, sells = [s0], []
    trips: list[dict[str, Any]] = []
    weights: list[float] = []
    b = p = s0
    floor: float | None = None

    def advance() -> None:
        """Record the previous close's position before any current-close trade."""
        nonlocal p
        p += 1
        held[p] = shares > 0
        weights.append(shares * close[p - 1] / values[-1] if values[-1] > 0 else 0.0)
        values.append(shares * close[p] + cash)

    while b < n - 1:
        fired = fire("x2", data, b)
        if floor is not None:
            under = next((d for d in range(b + 1, n) if close[d] < floor), None)
            if under is not None and (fired is None or under < fired[0]):
                fired = (under, "L")
        if fired is None or fired[0] + 1 > n - 1:
            break
        d, reason = fired
        v = d + 1
        while p < v:
            advance()
        cash = shares * close[v] * (1.0 - cb.COST)
        shares = 0.0
        values[-1] = cash
        sells.append(v)

        x = close[v]
        start = cash
        signal = signal_after(close, entries, moves, v)
        pending = list(LEVELS)
        fills: list[dict[str, Any]] = []
        units = 0.0
        how = "none"
        rest: float | None = None
        floor = None
        while p < n - 1 and how == "none":
            advance()
            c = close[p]
            filled = False
            for level, part in list(pending):
                if c <= level * x:
                    pending.remove((level, part))
                    spend = cash if not pending else part * start
                    shares += spend * (1.0 - cb.COST) / c
                    cash -= spend
                    units += spend / c
                    fills.append(
                        {
                            "level": level,
                            "date": data.dates[p].isoformat(),
                            "share": part,
                            "at": c / x,
                        }
                    )
                    filled = True
            if filled:
                buys.append(p)
            if not pending:
                how = "ladder"
            elif fills and c >= x:
                how = "recovery"
            elif signal is not None and p == signal[0]:
                how, floor = signal[1], signal[2]
            if how in ("recovery", "entry", "shift"):
                rest = cash / start
                shares += cash * (1.0 - cb.COST) / c
                units += cash / c
                cash = 0.0
                if buys[-1] != p:
                    buys.append(p)
            values[-1] = shares * c + cash

        if how == "none":
            units += cash / close[n - 1]
        trips.append(
            {
                "decision": data.dates[d].isoformat(),
                "reason": reason,
                "sell": data.dates[v].isoformat(),
                "buy": data.dates[p].isoformat() if how != "none" else None,
                "how": how,
                "days_out": p - v,
                "change": start / units / x - 1.0,
                "fills": fills,
                "rest_share": rest,
            }
        )
        if how == "none":
            break
        b = p
    while p < n - 1:
        advance()
    exposure = sum(weights) / len(weights) if weights else None
    return Ladder(
        tuple(values), cb.Route(tuple(held), tuple(buys), tuple(sells), tuple(trips)), exposure
    )


def span_result(series: ec.Closes, first_in: int) -> dict[str, Any]:
    """Preserve the prior span and append only the two authorized scaled paths."""
    block = sp.span_result(series, first_in)
    dates, close = series.dates, series.close
    entries = sp.v0_entries(close, first_in)
    s0 = max(first_in, ec.SMA_DAYS)
    prepared = xc.prepare(dates, close)
    line = ec.moving_average(close)
    moves = es.shifts(close, line, 5, s0)
    positions = {day.isoformat(): p for p, day in enumerate(dates)}
    span = block["span"]

    def fire_under(rule: str, data: xc.Prepared, b: int) -> tuple[int, str] | None:
        return sp.exit_under(rule, data, line, b)

    for strategy, base in LADDER.items():
        path = walk_ladder(
            prepared, entries, moves, s0, cb.exit_day if base == "x2_s5" else fire_under
        )
        for trip in path.route.trips:
            d = positions[trip["decision"]]
            at = line[d]
            if at is None:
                raise ValueError("出场响的收盘缺 200 日线。")
            trip["vs_line"] = close[d] / at - 1.0
        span["strategies"][strategy] = {
            **cb.metrics(dates, path.values, path.route, s0),
            "by_shift": sum(t["how"] == "shift" for t in path.route.trips),
            "low_stops": sum(t["reason"] == "L" for t in path.route.trips),
            "trips": list(path.route.trips),
            "by_ladder": sum(t["how"] == "ladder" for t in path.route.trips),
            "filled_sales": sum(bool(t["fills"]) for t in path.route.trips),
            "exposure": path.exposure,
        }
        annual = cb.year_returns(dates, path.values, s0)
        for year in span["years"]:
            year[strategy] = annual[year["year"]]
    for strategy in sp.STRATEGIES:
        row = span["strategies"][strategy]
        row["by_ladder"] = 0
        row["filled_sales"] = 0
        row["exposure"] = row["in_market"]
    return block


def rule_block() -> dict[str, Any]:
    """Retain all prior rules and separately describe the scaled buy-back."""
    return {
        **sp.rule_block(),
        "ladder": {
            "strategies": list(LADDER),
            "bases": dict(LADDER),
            "spans": list(SPANS),
            "levels": [list(level) for level in LEVELS],
            "fill": "close_at_or_below_level_times_sale_close_buys_that_share_of_sale_cash",
            "recovery": "after_a_tranche_first_close_at_or_above_the_sale_close_buys_the_rest",
            "signal": "first_entry_or_shift_after_the_sale_buys_the_rest_tie_is_entry",
            "order": ["levels", "ladder", "recovery", "entry", "shift"],
            "after": ["X2", "L"],
            "low_stop": "kept_when_a_shift_buys_the_rest",
            "partial": "no_sale_before_full",
            "restart": "x2_high_from_the_full_close",
            "cost": "per_trade_share_of_traded_value",
        },
    }


def check_table(
    data: BenchData, loaded: LoadedSettings, gate: tr.TrialGate, window: Mapping[str, Any]
) -> dict[str, Any]:
    """Reuse the QQQ input check without computing strategy outcomes."""
    q = sp.qqq_inputs(data, loaded, gate, window)
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": {"qqq": {"snapshot_id": data.source.get("snapshot_id")}},
        "rule": rule_block(),
        "check": {"qqq": q.check},
        "ok": q.check["ok"],
    }


def run_table(
    data: BenchData, loaded: LoadedSettings, gate: tr.TrialGate, window: Mapping[str, Any]
) -> dict[str, Any]:
    """Expose spans only after input and both generations of reprint checks pass."""
    q = sp.qqq_inputs(data, loaded, gate, window)
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "source": {"qqq": {"snapshot_id": data.source.get("snapshot_id")}},
        "rule": rule_block(),
        "check": {"qqq": q.check, "dev": None, "periods": None},
        "complete": False,
    }
    if not q.check["ok"]:
        return result
    spans = {
        "dev": span_result(q.dev, q.dev_first_in),
        "qqq": span_result(q.full, q.first_in),
    }
    result["check"]["dev"] = sp.dev_check(spans["dev"])
    periods = {name: rb.periods_check(name, spans[name]) for name in SPANS}
    result["check"]["periods"] = periods
    if not result["check"]["dev"]["ok"] or not all(c["ok"] for c in periods.values()):
        return result
    result["spans"] = spans
    result["complete"] = True
    return result


def fills_text(fills: Sequence[Mapping[str, Any]]) -> str:
    """List each filled tranche once with its execution date."""
    return "、".join(f"{f['level']:.1f}X {f['date']}" for f in fills) or "—"


def full_text(trip: Mapping[str, Any]) -> str:
    """Describe completion and the remaining cash share when tranches preceded it."""
    text = FULL_TEXT[trip["how"]]
    if trip["fills"] and trip["rest_share"] is not None:
        text += f"（剩下 {cb.share_text(trip['rest_share'])}）"
    return text


def _line(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def render_span(name: str, block: Mapping[str, Any]) -> list[str]:
    """Render selected strategy rows, annual returns, and both sets of sale details."""
    span = block["span"]
    lines = [
        f"## {sp.SPAN_TEXT[name]}",
        "",
        f"窗口 `{sp.SPAN_WINDOW[name]}`；满仓起点 {span['first_day']}，到 {span['last_day']}，"
        f"共 {span['sessions']} 个交易日。v0_close 入场 {len(span['entries'])} 次。",
        "",
    ]
    headers = [
        "策略",
        "总收益",
        "年化",
        "最大回撤",
        "回撤高点 → 低点",
        "最差 5% 的 20 日收益",
        "在场时间",
        "平均仓位",
        "卖出次数",
        "到过 0.9X",
        "四档买满 / 回到卖价 / 靠入场 / 结构转变 / 没买满",
        "跌回 L 下卖出",
        "买回更便宜",
        "平均买回差价",
        "平均空仓天数",
    ]
    lines += ["### 策略", "", _line(headers), "|" + "---|" * len(headers)]
    for strategy in SHOWN:
        row = span["strategies"][strategy]
        lines.append(
            _line(
                [
                    STRATEGY_TEXT[strategy],
                    xr.pct(row["total"]),
                    xr.pct(row["cagr"]),
                    xr.pct(row["max_drawdown"]),
                    f"{row['drawdown_peak']} → {row['drawdown_trough']}",
                    xr.pct(row["worst20"]),
                    cb.share_text(row["in_market"]),
                    cb.share_text(row["exposure"]),
                    str(row["sells"]),
                    str(row["filled_sales"]),
                    f"{row['by_ladder']} / {row['by_recovery']} / {row['by_entry']} / "
                    f"{row['by_shift']} / {row['not_back']}",
                    str(row["low_stops"]),
                    str(row["cheaper"]),
                    xr.pct(row["change_mean"]),
                    cb.days_text(row["days_out_mean"]),
                ]
            )
        )
    year_headers = ["年", *(STRATEGY_TEXT[s] for s in SHOWN)]
    lines += [
        "",
        "每年（从上一年最后一个收盘算，第一年从满仓起点算）：",
        "",
        _line(year_headers),
        "|" + "---|" * len(year_headers),
    ]
    for row in span["years"]:
        lines.append(_line([str(row["year"]), *(xr.pct(row[s]) for s in SHOWN)]))
    ladder_headers = [
        "策略",
        "出场响",
        "原因",
        "离 200 日线",
        "卖出",
        "成交的档",
        "买满",
        "怎么买满",
        "空仓到买满天数",
        "平均买回差价",
    ]
    lines += [
        "",
        "分批两行的每次卖出（原因 L = 收盘跌回 L 下；没买满的剩余现金按最后一个收盘算）：",
        "",
        _line(ladder_headers),
        "|" + "---|" * len(ladder_headers),
    ]
    for strategy in LADDER:
        for trip in span["strategies"][strategy]["trips"]:
            lines.append(
                _line(
                    [
                        STRATEGY_TEXT[strategy],
                        trip["decision"],
                        trip["reason"],
                        xr.pct(trip["vs_line"]),
                        trip["sell"],
                        fills_text(trip["fills"]),
                        trip["buy"] if trip["buy"] is not None else "—",
                        full_text(trip),
                        str(trip["days_out"]),
                        xr.pct(trip["change"]),
                    ]
                )
            )
    trip_headers = [
        "策略",
        "出场响",
        "原因",
        "离 200 日线",
        "卖出",
        "买回",
        "怎么买回",
        "空仓天数",
        "买回差价",
    ]
    lines += [
        "",
        "两条底的每次卖出和买回（原因 L = 收盘跌回 L 下；没买回的差价算到最后一个收盘）：",
        "",
        _line(trip_headers),
        "|" + "---|" * len(trip_headers),
    ]
    for strategy in TRIPS_SHOWN:
        for trip in span["strategies"][strategy]["trips"]:
            lines.append(
                _line(
                    [
                        STRATEGY_TEXT[strategy],
                        trip["decision"],
                        trip["reason"],
                        xr.pct(trip["vs_line"]),
                        trip["sell"],
                        trip["buy"] if trip["buy"] is not None else "—",
                        HOW_TEXT[trip["how"]],
                        str(trip["days_out"]),
                        xr.pct(trip["change"]),
                    ]
                )
            )
    return lines


def render_markdown(result: Mapping[str, Any]) -> str:
    """Print the two spans without interpretation or verdicts."""
    lines = [
        "# 结构转变策略的分批买回（TRADING-2565-126）",
        "",
        RULE_TEXT,
        "",
        NOTE,
        "",
    ]
    for name in SPANS:
        lines += render_span(name, result["spans"][name]) + [""]
    return "\n".join(lines).rstrip() + "\n"


def summary_line(result: Mapping[str, Any], out: Path) -> str:
    """Summarize all selected totals with the post-development span first."""
    parts = []
    for name in ("qqq", "dev"):
        rows = result["spans"][name]["span"]["strategies"]
        shown = "、".join(f"{s} {xr.pct(rows[s]['total'])}" for s in SHOWN)
        parts.append(f"{name} 总收益 {shown}")
    return f"structure_shift_ladder: {'；'.join(parts)}；结果目录 {out}"


def check_line(check: Mapping[str, Any], out: Path) -> str:
    """Report the QQQ input status and its coverage."""
    q = check["check"]["qqq"]
    return (
        f"structure_shift_ladder check: qqq {'ok' if q['ok'] else 'FAIL'}，"
        f"2011-01-03..{q['last_close_date']}，"
        f"{q['sessions']} 个交易日，缺 {q['missing_days']} 天；结果目录 {out}"
    )


def write_check(check: Mapping[str, Any], out: Path) -> None:
    """Write only the finite UTF-8 input-check artifact."""
    text = json.dumps(check, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(CHECK_JSON).write_text(text, encoding="utf-8")


def write_result(result: Mapping[str, Any], out: Path) -> None:
    """Retain failed checks as JSON, rendering tables only on completion."""
    text = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(RESULT_JSON).write_text(text, encoding="utf-8")
    if result["complete"]:
        out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """Print the authorized scaled buy-back controls."""


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option(help="输入检查目录，已有检查拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if out.joinpath(CHECK_JSON).exists():
        typer.echo("structure_shift_ladder: 已有检查结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded, window = sp.load_qqq(root)
        check = check_table(data, loaded, gate, window)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"structure_shift_ladder: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_check(check, out)
    typer.echo(check_line(check, out))
    if not check["ok"]:
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="策略结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("structure_shift_ladder: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, gate, loaded, window = sp.load_qqq(root)
        result = run_table(data, loaded, gate, window)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        lines = str(exc).splitlines()
        message = lines[0] if lines else ""
        typer.echo(f"structure_shift_ladder: 数据载入失败：{message}")
        raise typer.Exit(code=1) from None
    write_result(result, out)
    if not result["complete"]:
        stage = "输入检查" if result["check"]["dev"] is None else "重算核对"
        typer.echo(f"structure_shift_ladder: {stage} FAIL，没有印新行")
        raise typer.Exit(code=1)
    typer.echo(summary_line(result, out))


if __name__ == "__main__":
    app()
