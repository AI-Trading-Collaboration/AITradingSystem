"""第 5 期章程第 3.1 节：美股 1929–2010、QQQ 1999–2010 两个开发窗口。

规则没有记忆，借 H-A 的腿、模拟器、段表、拆分、停下核对和图函数。
两条规则的减仓事件和滞后共用一条减仓线。同一次运行的 fast_slow 必须和决定表
相同，且两条规则都逐日复现评估台才读。一次 run，没有 reference。
只输出描述表和图，不读验证和一致性窗口，不写研究账本、记录或试跑，
不开题、不改门和规则，不授权生产或券商动作。
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import trial
from ai_trading_system.research_bench.checks import (
    check_no_lookahead,
    check_parameters_consumed,
    cut_dates,
)
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.events import derisk_events
from ai_trading_system.research_bench.metrics import compare, summarize
from ai_trading_system.research_bench.prereg import StrategyEntry
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.segments import segment_section, segments, window_returns
from ai_trading_system.research_bench.settings import EpisodeSpec, LoadedSettings, Window
from ai_trading_system.research_bench.simulate import Rebalance, Simulation, schedule_mask, simulate
from ai_trading_system.research_data import qqq_long_history
from ai_trading_system.research_probes import asymmetric_reentry as ar
from ai_trading_system.research_probes import gate_power, plot_run
from ai_trading_system.research_probes import policy_table as pt

RUN_ID = "TRADING-2565-56"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-23:research_loop_p5"
DATA_TRIAL = ar.DATA_TRIAL
MARKETS = ar.MARKETS
MARKET_TEXT = pt.MARKET_TEXT
COST_BPS = pt.COST_BPS
QQQ_BULL = ar.QQQ_BULL
RULES = {"fast_slow": "月频 fast_slow", "slow_return": "慢腿快回"}
RUN_JSON = "slow_return.json"
RUN_MD = "slow_return.md"
FIGURES = "figures"
# 章程第 3.1 节机械判法；本探针只印，不据此晋级。
# D 和年化差是小数，报告印点，0.08 = 8 点。
SLOW_MEDIAN = 0.08
SLOW_POSITIVE = 8
BULL_RETAIN = 0.5
EXTRA_EXIT_SHARE = 0.5
SAME_RUN = 1e-12  # 同一次运行内两条计算路径的浮点误差容限，不是研究接受门槛。
RULE_TEXT = (
    "月底判，快腿 = 21 日 sign 分数，慢腿 = max（21 日分数，252 日分数），两腿各半，"
    "两个月底之间不变；分数按评估台 sign（收益恰为 0 记 0.5，不按章程「≥ 0 记 1」），"
    "max 对 0.5 照算"
)
Progress = Callable[[str, str, dict[str, Any]], None]
app = typer.Typer(help="慢腿的回来条件：只印开发窗口读数，不判、不写研究账本。")


def decide(scores: pd.DataFrame) -> pd.DataFrame:
    ends = schedule_mask(pd.DatetimeIndex(scores.index), "month_end")
    f, s, r = math.nan, math.nan, False
    rows = []
    for p, (fast, slow) in enumerate(scores[["fast", "slow"]].itertuples(index=False, name=None)):
        if ends[p] and math.isfinite(fast) and math.isfinite(slow):
            f, s, r = float(fast), float(max(fast, slow)), bool(fast > slow)
        rows.append((fast, slow, f, s, 0.5 * f + 0.5 * s, ends[p], r))
    return pd.DataFrame(
        rows,
        index=scores.index,
        columns=["fast_score", "slow_score", "fast", "slow", "target", "trade", "r"],
    )


def slow_return_params(entry: StrategyEntry) -> dict[str, Any]:
    return {"signals": [dict(s) for s in entry.params["signals"]]}


def slow_return_targets(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    if set(params) != {"signals"}:
        raise ValueError("params need signals only")
    return pd.Series(
        decide(ar.leg_scores(inputs, params["signals"]))["target"].to_numpy(), index=inputs.index
    )


def probe_path(
    rule: str, data: BenchData, scores: pd.DataFrame, window: Window, loaded: LoadedSettings
) -> ar.RulePath:
    if rule == "fast_slow":
        decided = ar.decide(scores, "monthly")
    elif rule == "slow_return":
        decided = decide(scores)
    else:
        raise ValueError(f"unknown rule {rule}")
    sim = ar.simulate_on(
        data.returns,
        decided["target"],
        decided["trade"],
        start=window.start,
        end=window.end,
        cost_bps=loaded.cost_bps,
    )
    legs = ar.held_legs(decided, sim)
    return ar.RulePath(rule, decided, sim, legs, plot_run.leg_moves(legs))


def bench_slow_return(
    data: BenchData, scores: pd.DataFrame, window: Window, loaded: LoadedSettings
) -> Simulation:
    return simulate(
        data.returns,
        0.5 * scores["fast"] + 0.5 * np.maximum(scores["fast"], scores["slow"]),
        start=window.start,
        end=window.end,
        rebalance=Rebalance("month_end", 0.0),
        cost_bps=loaded.cost_bps,
    )


def equivalence(
    data: BenchData,
    entry: StrategyEntry,
    paths: Mapping[str, ar.RulePath],
    window: Window,
    loaded: LoadedSettings,
) -> dict[str, bool]:
    return {
        "fast_slow": ar.same_days(
            paths["fast_slow"].sim, ar.bench_simulation(data, entry, window, loaded)
        ),
        "slow_return": ar.same_days(
            paths["slow_return"].sim,
            bench_slow_return(data, ar.scores_of(paths["slow_return"]), window, loaded),
        ),
    }


def zero_scores(decided: pd.DataFrame, window: Window) -> dict[str, int]:
    dates = pd.DatetimeIndex(decided.index)
    mask = (dates >= pd.Timestamp(window.start)) & (
        True if window.end is None else dates <= pd.Timestamp(window.end)
    )
    mask &= decided["trade"].to_numpy(dtype=bool)
    mask &= np.isfinite(decided[["fast_score", "slow_score"]].to_numpy(dtype=float)).all(axis=1)
    rows = decided.loc[mask]
    fast, slow = rows["fast_score"], rows["slow_score"]
    charter = 0.5 * (fast >= 0.5) + 0.5 * ((fast >= 0.5) | (slow >= 0.5))
    return {
        "month_ends": len(rows),
        "fast": int((fast == 0.5).sum()),
        "slow": int((slow == 0.5).sum()),
        "differs": int((charter != rows["target"]).sum()),
    }


def checks(data: BenchData, entry: StrategyEntry, window: Window) -> list[str]:
    index = pd.DatetimeIndex(data.inputs.index)
    start = pd.Timestamp(window.start)
    end = index[-1] if window.end is None else pd.Timestamp(window.end)
    params = slow_return_params(entry)
    return (
        check_no_lookahead(
            slow_return_targets, data.inputs, params, cut_dates(index, start, end, 8)
        )
        + check_parameters_consumed(slow_return_targets, data.inputs, params, start, end)[0]
    )


def _close(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return math.isfinite(left) and math.isfinite(right) and abs(left - right) <= SAME_RUN


def same_run(
    path: ar.RulePath,
    data: BenchData,
    loaded: LoadedSettings,
    gate: trial.TrialGate,
    market: trial.Market,
    window: Window,
    row: Mapping[str, Any],
) -> list[str]:
    if row.get("status") != "ok":
        return ["评估台的 fast_slow 没有算出"]
    matched = ar.matched_simulation(data, path.sim, window, loaded.cost_bps)
    cash = data.returns["cash"]
    comparison = compare(
        summarize(path.sim, cash, loaded), summarize(matched, cash, loaded), loaded
    )
    slow, bull = gate.markets[market].segment_lists(window.window_id)
    tables = pt.segment_tables(segment_section(path.sim, matched, slow, bull))
    different = []
    if not _close(
        comparison["geometric_annual_return"], row["comparison"]["geometric_annual_return"]
    ):
        different.append("同平均敞口几何差")
    for kind, key in (("slow_bear", "difference"), ("long_bull", "annual_difference")):
        actual, expected = tables[kind], row["segments"][kind]
        if [s["id"] for s in actual] != [s["id"] for s in expected]:
            different.append(f"{kind} 段清单")
            continue
        different.extend(
            a["id"] for a, b in zip(actual, expected, strict=True) if not _close(a[key], b[key])
        )
    return different


def event_table(
    sim: Simulation,
    data: BenchData,
    loaded: LoadedSettings,
    episodes: Sequence[EpisodeSpec],
    reference_exposure: float,
) -> dict[str, Any]:
    return derisk_events(
        sim.exposure,
        data.returns["qqq"].reindex(sim.exposure.index),
        reference_exposure=reference_exposure,
        settings=load_screen_settings().settings.derisk_events,
        episodes=tuple(episodes),
        trading_days_per_year=loaded.settings.trading_days_per_year,
    )


def event_summary(events: Mapping[str, Any]) -> dict[str, Any]:
    return {k: events[k] for k in ("count", "per_year", "short_count")} | {
        k: v["count"] for k, v in events["outcomes"].items()
    }


def lag_table(
    path: ar.RulePath,
    data: BenchData,
    loaded: LoadedSettings,
    slow: Sequence[EpisodeSpec],
    reference_exposure: float,
) -> dict[str, Any]:
    episodes = event_table(path.sim, data, loaded, slow, reference_exposure)["episodes"]
    found = {}
    for segment in slow:
        counts = plot_run.count_moves(path.moves, segment.start, segment.end)
        reading = episodes.get(segment.id, {})
        found[segment.id] = {
            "exit_lag_days": reading.get("exit_lag_days"),
            "reentry_lag_days": reading.get("reentry_lag_days"),
            **{
                f"{leg}_{kind}": counts[leg][kind]
                for leg in plot_run.LEGS
                for kind in plot_run.KINDS
            },
        }
    return found


def _held(path: ar.RulePath) -> np.ndarray[Any, Any]:
    days = path.sim.daily.index
    positions = path.decided.index.get_indexer(days)
    if (positions < 0).any():
        raise ValueError("a return date holds no decision")
    held = ar._held_positions(path.decided, 0)[positions]
    if (held < 0).any():
        raise ValueError("a return date holds no decision")
    return held


def r_cell(
    path: ar.RulePath, asset: pd.Series, start: str | None = None, end: str | None = None
) -> dict[str, Any]:
    held = _held(path)
    mask = path.decided["r"].to_numpy(dtype=bool)[held]
    months = pd.DatetimeIndex(path.sim.daily.index).to_period("M")
    if start is not None:
        mask &= months >= pd.Period(start, "M")
    if end is not None:
        mask &= months <= pd.Period(end, "M")
    return {
        "count": len(np.unique(held[mask])),
        "return": float((1 + asset.reindex(path.sim.daily.index).loc[mask]).prod() - 1)
        if mask.any()
        else None,
    }


def r_window(path: ar.RulePath, asset: pd.Series) -> dict[str, Any]:
    held = _held(path)
    returns = asset.reindex(path.sim.daily.index)
    monthly = (1 + returns).groupby(held).prod() - 1
    r = path.decided["r"].iloc[monthly.index.to_numpy(dtype=int)].to_numpy(dtype=bool)
    count, r_count = len(monthly), int(r.sum())
    return {
        "months": count,
        "r_months": r_count,
        "share": r_count / count if count else None,
        "r_mean": float(monthly.loc[r].mean()) if r_count else None,
        "all_mean": float(monthly.mean()) if count else None,
    }


def split_table(
    paths: Mapping[str, ar.RulePath], asset: pd.Series, bulls: Sequence[EpisodeSpec]
) -> dict[str, Any]:
    found = {}
    for name, path in paths.items():
        rows = ar.split_rows(ar.missed_gains(path.decided, asset, path.sim.daily.index), bulls)
        pooled = ar.pooled(rows)
        if name == "fast_slow":
            for row in [*rows, pooled]:
                row["band_ceiling"] = ar.lag_share(
                    row["false_exit"], row["false_exit"] + row["reentry_lag"] + row["slow"]
                )
        found[name] = {"rows": rows, "pooled": pooled}
    return found


def _bull_cell(row: Mapping[str, Any]) -> Any:
    return next(
        (s["annual_difference"] for s in row["segments"]["long_bull"] if s["id"] == QQQ_BULL), None
    )


def row_readings(
    new: Mapping[str, Any], base: Mapping[str, Any], exits: Mapping[str, Any]
) -> list[dict[str, Any]]:
    summary = new["us_market"]["segments"]["slow_bear_summary"]
    us = {k: summary[k] for k in ("median_difference", "positive", "count")}
    differences = [s["difference"] for s in new["qqq"]["segments"]["slow_bear"]]
    bull, old_bull = _bull_cell(new["qqq"]), _bull_cell(base["qqq"])
    threshold = None if old_bull is None else old_bull * BULL_RETAIN
    geometric = new["us_market"]["comparison"]["geometric_annual_return"]
    old_geometric = base["us_market"]["comparison"]["geometric_annual_return"]
    exit_reading = {
        m: {"extra": exits[m]["extra"], "fast_slow_exits": exits[m]["fast_slow"]["exits"]}
        for m in MARKETS
    }
    return [
        {
            "row": 1,
            "reading": us,
            "threshold": "D 中位 ≥ +8.0 且 D > 0 ≥ 8 段",
            "holds": us["median_difference"] is not None
            and us["median_difference"] >= SLOW_MEDIAN
            and us["positive"] >= SLOW_POSITIVE,
        },
        {
            "row": 2,
            "reading": {"differences": differences},
            "threshold": "两段 D 都 > 0",
            "holds": len(differences) == 2 and all(d is not None and d > 0 for d in differences),
        },
        {
            "row": 3,
            "reading": {"slow_return": bull, "fast_slow": old_bull},
            "threshold": threshold,
            "holds": bull is not None and threshold is not None and bull >= threshold,
        },
        {
            "row": 4,
            "reading": {
                "slow_return": geometric,
                "fast_slow": old_geometric,
                "cost_25": {
                    "slow_return": new["us_market"]["cost"]["difference"],
                    "fast_slow": base["us_market"]["cost"]["difference"],
                },
            },
            "threshold": old_geometric,
            "holds": geometric is not None
            and old_geometric is not None
            and geometric >= old_geometric,
        },
        {
            "row": 5,
            "reading": exit_reading,
            "threshold": "两个窗口：多出的退出 ≤ fast_slow 退出 / 2",
            "holds": all(
                r["extra"] <= r["fast_slow_exits"] * EXTRA_EXIT_SHARE for r in exit_reading.values()
            ),
        },
    ]


def _line_text(line: Mapping[str, Any]) -> str:
    reference = line["reference_exposure"]
    return (
        f"减仓线：fast_slow 平均敞口 {reference:.3f}；减仓 ≤ {line['ratio']:.2f} × "
        f"{reference:.3f} = {line['threshold']:.3f}，回来 ≥ {reference:.3f}；"
        "两条规则共用（第 5 期章程第 6 节）"
    )


def draw(
    market: trial.Market,
    paths: Mapping[str, ar.RulePath],
    results: Mapping[str, Any],
    lags: Mapping[str, Any],
    events: Mapping[str, Any],
    line: Mapping[str, Any],
    data: BenchData,
    loaded: LoadedSettings,
    gate: trial.TrialGate,
    window: Window,
    provenance: Mapping[str, Any],
    out_dir: Path,
) -> list[dict[str, Any]]:
    slow, bull = gate.markets[market].segment_lists(window.window_id)
    bands = [
        plot_run.SegmentBand(s.id, kind, s.start, s.end)
        for kind, specs in (("slow_bear", slow), ("long_bull", bull))
        for s in specs
    ]
    bands += [
        plot_run.SegmentBand(s.id, s.kind, s.start, s.end)
        for s in segments(window_returns(data.returns["qqq"], window))
        if s.kind == "fast_drop"
    ]
    footer = [
        f"{RUN_ID}；{MARKET_TEXT[market]}；窗口 {window.window_id} "
        f"{window.start.isoformat()}..{window.end.isoformat() if window.end else ''}；"
        f"提交 {provenance['commit']}；快照 "
        + "、".join(
            str(data.source[k]) for k in ("snapshot_id", "signal_snapshot_id") if data.source.get(k)
        ),
        "竖线：每条腿每次出入一根，落在生效日（t 收盘定、t+1 收盘成交、t+2 持有）的指数上；"
        "出场朝上 ▼，入场朝下 ▲；慢腿长、快腿短；灰 = 月频 fast_slow，蓝 = 慢腿快回。",
        _line_text(line),
    ]
    rules = []
    readings: dict[str, dict[str, plot_run.SegmentReading]] = {}
    roles: tuple[plot_run.Role, ...] = ("base", "compared")
    for role, name in zip(roles, RULES, strict=True):
        path = paths[name]
        rules.append(
            plot_run.RuleSeries(
                RULES[name],
                role,
                path.sim.exposure,
                path.sim.returns,
                path.legs,
                ar.matched_simulation(data, path.sim, window, loaded.cost_bps).returns,
            )
        )
        for kind, key in (("slow_bear", "difference"), ("long_bull", "annual_difference")):
            for segment in results[name]["segments"][kind]:
                lag = lags[name].get(segment["id"], {})
                readings.setdefault(segment["id"], {})[role] = plot_run.SegmentReading(
                    segment.get(key), lag.get("exit_lag_days"), lag.get("reentry_lag_days")
                )
        event = events[name]
        footer.append(
            f"{RULES[name]}：减仓事件 {event['count']}，每年 {event['per_year']:.2f}，"
            f"短事件 {event['short_count']}（共用减仓线，评估台事件表口径）"
        )
    asset = data.returns["qqq"].reindex(next(iter(paths.values())).sim.daily.index)
    spec = plot_run.FigureSpec(
        "run",
        market,
        f"{MARKET_TEXT[market]}，月频 fast_slow 对 慢腿快回",
        window.window_id,
        asset,
        bands,
        rules,
        readings,
        footer,
    )
    return [
        {
            "name": r.name,
            "sha256": r.sha256,
            "bytes": r.bytes,
            "counts": gate_power._clean(r.counts),
        }
        for r in plot_run.figures(spec, out_dir / FIGURES)
    ]


def run_probe(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: trial.TrialGate,
    root_dir: Path,
    *,
    provenance: Mapping[str, Any],
    out_dir: Path,
    progress: Progress | None = None,
    decision_table: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    # 借 H-A 头部，保留同一次运行的基准、停下核对、回填一致、窗口和来源；题号另记。
    result = ar._begin(
        "run", datasets, loaded, gate, root_dir, provenance, progress, decision_table
    )
    result.pop("direction", None)
    result.update(run_id=RUN_ID, owner_decision=OWNER_DECISION)
    if result["stopped"]:
        return dict(gate_power._clean(result))
    for key in (
        "checks",
        "equivalence",
        "same_run",
        "slow_return",
        "derisk_line",
        "events",
        "zero_scores",
        "exits",
        "r_months",
        "r_window",
        "split",
        "lags",
        "figures",
    ):
        result[key] = {}
    entry = ar.source_entries(root_dir)["fast_slow"]
    for market in MARKETS:
        data = datasets[market]
        window = ar.market_window(loaded, gate, market)
        result["checks"][market] = checks(data, entry, window)
        scores = ar.leg_scores(data.inputs, entry.params["signals"])
        paths = {name: probe_path(name, data, scores, window, loaded) for name in RULES}
        eq = equivalence(data, entry, paths, window, loaded)
        result["equivalence"][market] = eq
        if not all(eq.values()) or result["checks"][market]:
            continue
        base = result["rules"][market]["fast_slow"]
        result["same_run"][market] = same_run(
            paths["fast_slow"], data, loaded, gate, market, window, base
        )
        if result["same_run"][market]:
            continue
        found = ar.evaluate_h_a(paths["slow_return"], data, loaded, gate, market, window)
        found.pop("events", None)
        found["row"] = "slow_return"
        result["slow_return"][market] = found
        if progress is not None:
            progress(market, "slow_return", found)
        reference = float(paths["fast_slow"].sim.exposure.mean())
        episodes = gate.markets[market].event_episodes
        tables = {
            name: event_table(
                path.sim,
                data,
                loaded,
                loaded.settings.episodes if episodes is None else episodes,
                reference,
            )
            for name, path in paths.items()
        }
        result["derisk_line"][market] = {
            "ratio": load_screen_settings().settings.derisk_events.derisk_ratio,
            "reference_exposure": reference,
            "threshold": tables["fast_slow"]["threshold"],
        }
        result["events"][market] = {name: event_summary(t) for name, t in tables.items()}
        result["zero_scores"][market] = zero_scores(paths["slow_return"].decided, window)
        counts = {name: ar.exit_counts(path.moves) for name, path in paths.items()}
        result["exits"][market] = {
            **counts,
            "extra": counts["slow_return"]["exits"] - counts["fast_slow"]["exits"],
        }
        slow, bull = gate.markets[market].segment_lists(window.window_id)
        asset = data.returns["qqq"]
        result["r_months"][market] = [
            {"id": s.id, "kind": kind, **r_cell(paths["slow_return"], asset, s.start, s.end)}
            for kind, specs in (("slow_bear", slow), ("long_bull", bull))
            for s in specs
        ]
        result["r_window"][market] = r_window(paths["slow_return"], asset)
        result["split"][market] = split_table(paths, asset, bull)
        result["lags"][market] = {
            name: lag_table(path, data, loaded, slow, reference) for name, path in paths.items()
        }
        result["figures"][market] = draw(
            market,
            paths,
            {"fast_slow": base, "slow_return": found},
            result["lags"][market],
            result["events"][market],
            result["derisk_line"][market],
            data,
            loaded,
            gate,
            window,
            provenance,
            out_dir,
        )
    result["complete"] = all(m in result["slow_return"] for m in MARKETS)
    result["readings"] = (
        row_readings(
            result["slow_return"],
            {m: result["rules"][m]["fast_slow"] for m in MARKETS},
            result["exits"],
        )
        if result["complete"]
        else []
    )
    result["first_four"] = ar.first_four(result["readings"])
    return dict(gate_power._clean(result))


def _points2(value: Any) -> str:
    return "—" if value is None else f"{value * 100:+.2f}"


def _reading_text(row: Mapping[str, Any]) -> str:
    r, number = row["reading"], row["row"]
    if number == 1:
        return f"D 中位 {pt._points(r['median_difference'])}，D > 0 {r['positive']}/{r['count']}"
    if number == 2:
        return "、".join(pt._points(d) for d in r["differences"])
    if number in (3, 4):
        text = f"慢腿快回 {_points2(r['slow_return'])}，fast_slow {_points2(r['fast_slow'])}"
        if number == 4:
            cost = r["cost_25"]
            text += (
                f" 点/年（25 bps 下 {_points2(cost['slow_return'])}、"
                f"{_points2(cost['fast_slow'])}，只印）"
            )
        return text
    return "；".join(
        f"{MARKET_TEXT[m]}：多出 {r[m]['extra']}，fast_slow 退出 {r[m]['fast_slow_exits']}"
        for m in MARKETS
    )


def _threshold(row: Mapping[str, Any]) -> str:
    value = row["threshold"]
    return "—" if value is None else f"≥ {_points2(value)}" if row["row"] in (3, 4) else str(value)


def _holds(value: Any) -> str:
    return "—" if value is None else "成立" if value else "不成立"


def _pct(value: Any) -> str:
    return "—" if value is None else f"{value * 100:+.1f}%"


def _table(
    lines: list[str], title: str, headers: Sequence[str], rows: Sequence[Sequence[Any]]
) -> None:
    lines.extend(["", title, "", pt._table_row(headers), "|" + "---|" * len(headers)])
    lines.extend(pt._table_row(row) for row in rows)


def _rules(result: Mapping[str, Any], market: str) -> dict[str, Any]:
    rules = dict(result["rules"][market])
    if market in result.get("slow_return", {}):
        rules["slow_return"] = result["slow_return"][market]
    return rules


def _main_report(lines: list[str], result: Mapping[str, Any]) -> None:
    rows = []
    for market in MARKETS:
        for name, row in _rules(result, market).items():
            if row["status"] != "ok":
                rows.append([MARKET_TEXT[market], RULES[name], "算不出"])
                continue
            own, comp, cost = row["strategy"], row["comparison"], row["cost"]
            rows.append(
                [
                    MARKET_TEXT[market],
                    RULES[name],
                    pt._with_difference(
                        own["geometric_annual_return"], comp["geometric_annual_return"]
                    ),
                    pt._with_difference(cost["strategy"], cost["difference"]),
                    pt._with_difference(own["max_drawdown"], comp["max_drawdown"]),
                    f"{own['cvar_5pct'] * 100:.2f}%"
                    f"（{pt._relative(comp['cvar_5pct_relative_improvement'])}）"
                    if own["cvar_5pct"] is not None
                    else "—",
                    f"{own['average_exposure']:.2f}",
                    f"{own['annual_turnover']:.2f}",
                ]
            )
    _table(
        lines,
        "## 主表（括号内对同平均敞口固定敞口）",
        [
            "市场",
            "规则",
            "几何年化",
            "25 bps 下几何年化",
            "最大回撤",
            "最差 5% 日均",
            "平均敞口",
            "年换手",
        ],
        rows,
    )
    _table(
        lines,
        "## R 月（快腿分数 > 慢腿分数的月底之后那个持有月，只印）",
        ["市场", "持有月", "R 月", "占比", "R 月平均月收益", "全部持有月平均月收益"],
        [
            [
                MARKET_TEXT[m],
                r["months"],
                r["r_months"],
                *[_pct(r[k]) for k in ("share", "r_mean", "all_mean")],
            ]
            for m, r in result.get("r_window", {}).items()
        ],
    )
    for market in MARKETS:
        rules = _rules(result, market)
        for kind, key, title in (
            ("slow_bear", "difference", "慢熊段 D（点）"),
            ("long_bull", "annual_difference", "长牛段年化差（点/年）"),
        ):
            cells = {
                n: {s["id"]: s for s in row.get("segments", {}).get(kind, [])}
                for n, row in rules.items()
            }
            ids = list(dict.fromkeys(sid for table in cells.values() for sid in table))
            r_cells = {s["id"]: s for s in result.get("r_months", {}).get(market, [])}
            _table(
                lines,
                f"## {MARKET_TEXT[market]}：{title}，各对自己的同平均敞口固定敞口",
                ["段", *[RULES[n] for n in rules], "R 月个数", "R 月市场累计"],
                [
                    [
                        sid,
                        *[pt._points(cells[n].get(sid, {}).get(key)) for n in rules],
                        r_cells.get(sid, {}).get("count"),
                        _pct(r_cells.get(sid, {}).get("return")),
                    ]
                    for sid in ids
                ],
            )


def _details(lines: list[str], result: Mapping[str, Any]) -> None:
    rows = []
    for market, rules in result.get("split", {}).items():
        for name, split in rules.items():
            for r in [*split["rows"], *([split["pooled"]] if market == "us_market" else [])]:
                ceiling = r.get("band_ceiling")
                rows.append(
                    [
                        MARKET_TEXT[market],
                        RULES[name],
                        r["id"],
                        *[pt._points(r[k]) for k in ("false_exit", "reentry_lag", "slow")],
                        "—"
                        if name != "fast_slow"
                        else "不定义"
                        if ceiling is None
                        else f"{ceiling:.2f}",
                    ]
                )
    _table(
        lines,
        "## 长牛代价三份（带符号的 log 收益，点；H-A reference 同一算法，只印）",
        ["市场", "规则", "段", "假退出", "再入迟", "慢腿", "带宽退出的上限（假退出份额）"],
        rows,
    )
    _table(
        lines,
        "## 退出次数（按腿，生效日，全窗口）",
        ["市场", "规则", "快腿出", "慢腿出", "退出", "入场", "多出"],
        [
            [
                MARKET_TEXT[m],
                RULES[n],
                *[r[n][k] for k in ("fast_exits", "slow_exits", "exits", "entries")],
                r["extra"] if n == "slow_return" else "—",
            ]
            for m, r in result.get("exits", {}).items()
            for n in RULES
        ],
    )
    _table(
        lines,
        "## 减仓事件（两条规则共用减仓线，评估台事件表口径，全窗口）",
        ["市场", "规则", "减仓线", "事件", "每年", "短事件", "按结果"],
        [
            [
                MARKET_TEXT[m],
                RULES[n],
                f"{result['derisk_line'][m]['threshold']:.3f}",
                e["count"],
                f"{e['per_year']:.2f}",
                e["short_count"],
                "、".join(
                    f"{k} {v}"
                    for k, v in e.items()
                    if k not in ("count", "per_year", "short_count")
                ),
            ]
            for m, rules in result.get("events", {}).items()
            for n, e in rules.items()
        ],
    )
    _table(
        lines,
        "## 门的条件（只印，不判，这题不走试跑门，第 5 期章程第 3 节）",
        ["市场", "条件", "读数", "门槛", "印出"],
        [
            [
                MARKET_TEXT[m],
                r["metric"],
                r.get("observed"),
                f"{r['op']} {r['value']}",
                _holds(r.get("holds")),
            ]
            for m, row in result.get("slow_return", {}).items()
            for r in row["gate"]
        ],
    )
    _table(
        lines,
        "## 慢熊段的腿出入和滞后（交易日，共用减仓线）",
        ["市场", "规则", "段", "快腿出", "快腿入", "慢腿出", "慢腿入", "出场滞后", "回仓滞后"],
        [
            [
                MARKET_TEXT[m],
                RULES[n],
                sid,
                *[
                    r[k]
                    for k in (
                        "fast_exit",
                        "fast_entry",
                        "slow_exit",
                        "slow_entry",
                        "exit_lag_days",
                        "reentry_lag_days",
                    )
                ],
            ]
            for m, rules in result.get("lags", {}).items()
            for n, segs in rules.items()
            for sid, r in segs.items()
        ],
    )
    _table(
        lines,
        "## 图",
        ["文件", "sha256", "字节"],
        [
            [r["name"], r["sha256"], r["bytes"]]
            for records in result.get("figures", {}).values()
            for r in records
        ],
    )


def render(result: Mapping[str, Any]) -> str:
    sources = result["sources"]
    first = (
        "没有读完"
        if result["stopped"] or not result["complete"]
        else "是"
        if result["first_four"]
        else "否"
    )
    lines = [
        "# 慢腿的回来条件：读数",
        "",
        f"- 依据：{result['owner_decision']}（第 5 期章程第 3.1 节）；运行 {result['run_id']}；"
        "B 区探针，不判、不计数、不改门、不写研究账本，不读验证和一致性窗口。",
        "- 判法：第 5 期章程第 3.1 节判法表第 1 到 5 行，机械比较；对错由循环线程填进记分表。"
        f"第 1 到 4 行都成立：{first}",
        f"- 规则：{RULE_TEXT}。",
        f"- 提交 {result['provenance']['commit']}；数据：计划 {result['data_trial']} 的快照"
        f"（美股大盘 {sources['us_market'].get('signal_snapshot_id')}，"
        f"QQQ {sources['qqq'].get('snapshot_id')}），载入后截到 2010-12-31；"
        f"段清单取自门文件（sha256 {result['gate_sha256']}）。",
    ]
    for market, w in result["windows"].items():
        lines.append(
            f"- 窗口（{MARKET_TEXT[market]}）：{w['window_id']}，"
            f"请求 {w['start']}..{w['end'] or ''}；"
            f"实际评估 {w['evaluated_start']}..{w['evaluated_end']}；"
            "数据质量：载入入口校验，来源见 JSON sources。"
        )
    if result["stopped"]:
        for market, differences in result["stop"].items():
            if differences:
                lines.append(
                    f"- 同一次运行的 fast_slow 和决定表不同（{MARKET_TEXT[market]}）："
                    + "、".join(differences)
                )
        lines.append("- 按第 4 期章程第 3.1 节的停下核对停下：不读新规则，不出图。")
    else:
        lines.append(
            "- 同一次运行的 fast_slow 和决定表（TRADING-2565-42）相同（差都不超过 0.05 点）。"
        )
        for item in result["backfill"]:
            text = "一致" if item["consistent"] else "不一致：" + "、".join(item["differs"])
            lines.append(f"- 和回填检验（TRADING-2567-33）：{text}")
        for market, eq in result.get("equivalence", {}).items():
            text = (
                "两条规则的探针路径都逐日复现评估台"
                if all(eq.values())
                else "不等：" + "、".join(k for k, v in eq.items() if not v)
            )
            lines.append(f"- 等同核对（{MARKET_TEXT[market]}）：{text}")
        for market, problems in result.get("checks", {}).items():
            if problems:
                lines.append(
                    f"- 前视和参数核对（{MARKET_TEXT[market]}）没过：" + "、".join(problems)
                )
        for market, differences in result.get("same_run", {}).items():
            text = "不同：" + "、".join(differences) if differences else "同数"
            lines.append(f"- fast_slow 两条算法（{MARKET_TEXT[market]}）：{text}")
        for market, line in result.get("derisk_line", {}).items():
            lines.append(
                f"- {MARKET_TEXT[market]}：{_line_text(line)}；两条规则的减仓事件和滞后都用这条线。"
            )
        for market, z in result.get("zero_scores", {}).items():
            lines.append(
                f"- 收益恰为 0 的月底（{MARKET_TEXT[market]}，窗口里 {z['month_ends']} 个月底）："
                f"快腿分数 0.5 {z['fast']} 次，慢腿分数 0.5 {z['slow']} 次；"
                f"目标和章程写法不同 {z['differs']} 次。"
            )
        if result["complete"]:
            _table(
                lines,
                "## 第 1 到 5 行（第 5 期章程第 3.1 节判法，机械比较；对错由循环线程填进记分表）",
                ["行", "读数", "门槛", "按判法"],
                [
                    [r["row"], _reading_text(r), _threshold(r), _holds(r["holds"])]
                    for r in result["readings"]
                ],
            )
        _main_report(lines, result)
        _details(lines, result)
    return "\n".join([*lines, "", "production_effect=none；broker_action=none。", ""])


def write_json(result: Mapping[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(gate_power._clean(result), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def load_inputs(root: Path) -> tuple[dict[str, BenchData], trial.TrialGate, LoadedSettings, Path]:
    return pt.load_data(root)


def _load(root: Path) -> tuple[dict[str, BenchData], trial.TrialGate, LoadedSettings, Path]:
    try:
        return load_inputs(root)
    except (OSError, ValueError, KeyError, trial.TrialError) as exc:
        typer.echo(f"slow_leg_return: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None


def check_rows(
    datasets: Mapping[str, BenchData], loaded: LoadedSettings, gate: trial.TrialGate, root_dir: Path
) -> list[dict[str, Any]]:
    found = []
    for market in MARKETS:
        row: dict[str, Any] = {"market": market}
        try:
            window = ar.market_window(loaded, gate, market)
            entry = ar.source_entries(root_dir)["fast_slow"]
            data = datasets[market]
            scores = ar.leg_scores(data.inputs, entry.params["signals"])
            dates = pd.DatetimeIndex(scores.index)
            mask = (dates >= pd.Timestamp(window.start)) & (
                True if window.end is None else dates <= pd.Timestamp(window.end)
            )
            missing = int(scores.loc[mask].isna().any(axis=1).sum())
            paths = {name: probe_path(name, data, scores, window, loaded) for name in RULES}
            eq = equivalence(data, entry, paths, window, loaded)
            problems = checks(data, entry, window)
            row.update(
                window_id=window.window_id,
                missing_days=missing,
                equivalence=eq,
                problems=problems,
                ok=bool(mask.any()) and missing == 0 and all(eq.values()) and not problems,
            )
        except Exception as exc:
            row.update(ok=False, error=pt._error(exc))
        found.append(row)
    return found


def _print_checks(rows: Sequence[Mapping[str, Any]]) -> bool:
    for row in rows:
        prefix = f"slow_leg_return: {MARKET_TEXT[row['market']]}"
        if "error" in row:
            typer.echo(f"{prefix} 算不出：{row['error']}")
        else:
            eq = "、".join(f"{k} {'同' if v else '不同'}" for k, v in row["equivalence"].items())
            check = "过" if not row["problems"] else "没过：" + "、".join(row["problems"])
            typer.echo(
                f"{prefix} 窗口 {row['window_id']} 缺 {row['missing_days']} 天；等同核对 {eq}；"
                f"前视和参数核对 {check}；{'可以' if row['ok'] else '不行'}"
            )
    return all(row["ok"] for row in rows)


def _progress(market: str, name: str, row: dict[str, Any]) -> None:
    typer.echo(f"slow_leg_return: {market} {name} {row['status']}")


@app.callback()
def main() -> None:
    """只读开发窗口，一次 run，没有 reference。"""


@app.command("check")
def check_command(
    root: Annotated[Path, typer.Option()] = qqq_long_history.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    datasets, gate, loaded, root_dir = _load(root)
    if not _print_checks(check_rows(datasets, loaded, gate, root_dir)):
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option()],
    root: Annotated[Path, typer.Option()] = qqq_long_history.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any((out / name).exists() for name in (RUN_JSON, RUN_MD, FIGURES)):
        typer.echo("slow_leg_return: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    datasets, gate, loaded, root_dir = _load(root)
    if not _print_checks(check_rows(datasets, loaded, gate, root_dir)):
        typer.echo("slow_leg_return: 开跑前检查没过，停下。")
        raise typer.Exit(code=1)
    out.mkdir(parents=True, exist_ok=True)
    result = run_probe(
        datasets,
        loaded,
        gate,
        root_dir,
        provenance=git_provenance(PROJECT_ROOT),
        out_dir=out,
        progress=_progress,
    )
    write_json(result, out / RUN_JSON)
    (out / RUN_MD).write_text(render(result), encoding="utf-8")
    if result["stopped"] or not result["complete"]:
        typer.echo("slow_leg_return: 没有读完（停下核对、等同核对或两条算法不同）；见报告头部。")
        raise typer.Exit(code=1)
    for row in result["readings"]:
        typer.echo(f"slow_leg_return: 第 {row['row']} 行 {_holds(row['holds'])}")
    typer.echo(f"slow_leg_return: 完成；结果目录 {out}")


if __name__ == "__main__":
    app()
