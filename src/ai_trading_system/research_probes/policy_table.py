"""Describe three fixed exposures and four unchanged forward-record trend overlays.

Uses the bench unchanged on US market 1929–2010 and QQQ 1999–2010 development
windows. Validation cells are copied from published decisions; 1871–1928 cells
come only from a supplied century probe result, with monthly-equivalence caveats.
This is a descriptive policy table, not a gate, trial, research verdict or daily
production policy. It neither reads held-out data or research execution records,
counts trials, changes gates, writes ledgers, nor authorizes production/broker actions.
US market history is a proxy, not QQQ evidence; data checks do not confer eligibility.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from statistics import median
from typing import Annotated, Any, Literal

import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.metrics import compare, summarize
from ai_trading_system.research_bench.run import BenchCheckError, RunSpec, run_strategy
from ai_trading_system.research_bench.settings import (
    LoadedSettings,
    Window,
    load_settings,
    resolve_window,
)
from ai_trading_system.research_bench.simulate import Rebalance, constant_targets, simulate
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import backfill_check as bc
from ai_trading_system.research_probes import gate_power as gp

RUN_ID = "TRADING-2565-42"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-21:period3_direction"
DATA_TRIAL = bc.DATA_TRIAL
MARKETS: tuple[tr.Market, ...] = ("us_market", "qqq")
MARKET_TEXT = {"us_market": "美股大盘 1929–2010", "qqq": "QQQ 1999–2010"}
RESERVE = "cash"
FIXED_EXPOSURES = (0.6, 0.8, 1.0)
FIXED_REBALANCE = Rebalance(schedule="month_end", band=0.0)
# 需求第 1 节：固定敞口三档月底调仓；四个覆盖照回填检验配置原样取，前瞻记录 F0001–F0004。
OVERLAYS = {
    "fast_slow": "F0001",
    "trend4": "F0002",
    "slow_only": "F0003",
    "fast_slow_weekly": "F0004",
}
# 描述表沿用回填检验的 25 bps 成本情景，只印，不作为接受门槛。
COST_BPS = 25.0
METRICS = (
    "geometric_annual_return",
    "max_drawdown",
    "cvar_5pct",
    "average_exposure",
    "annual_turnover",
)
COMPARED = (
    "geometric_annual_return",
    "max_drawdown",
    "cvar_5pct_relative_improvement",
    "average_exposure",
    "annual_turnover",
)
CENTURY_RUN_ID = "TRADING-2565-41"
# 乙的月频等价配置，周频那条没有月频等价。
CENTURY = {"fast_slow": "fast_slow_m", "trend4": "trend4_m", "slow_only": "slow_only_m"}
BACKFILL_RUN_ID = "TRADING-2567-33"
# 浮点重算核对容差（需求指定），只标记差异，不拦截、不判策略。
BACKFILL_TOLERANCE = 1e-9
RESULT_JSON = "policy_table.json"
RESULT_MD = "policy_table.md"


@dataclass(frozen=True)
class Validation:
    question: str
    column: int
    run_id: str
    interval: str
    exposure: str
    drawdown: str
    cvar: str
    geometric: str
    cost: str


# 窗口 C 只抄已发布 decision.md 验证窗口表的原文。
# 不读运行记录，不读这段数据。
VALIDATION = {
    "fast_slow": Validation(
        "QQQ-Q3",
        1,
        "QQQ-Q3-r0008",
        "95% 平稳 bootstrap",
        "0.82",
        "+5.3 个点（−6.1..+8.4）",
        "+4.4%（−6.5..+14.3）",
        "−1.8 个点（−5.4..+1.5）",
        "−2.5 个点",
    ),
    "trend4": Validation(
        "QQQ-Q2",
        1,
        "QQQ-Q2-r0011",
        "97.5% 平稳 bootstrap",
        "0.82",
        "+0.3 个点（−7.9..+7.6）",
        "+0.4%（−7.3..+7.8）",
        "−2.9 个点（−6.8..+0.9）",
        "−3.3 个点",
    ),
    "slow_only": Validation(
        "QQQ-Q3",
        2,
        "QQQ-Q3-r0009",
        "95% 平稳 bootstrap",
        "0.97",
        "−0.7 个点（−5.1..−0.1）",
        "−2.3%（−2.7..−1.4）",
        "−1.3 个点（−3.6..+0.4）",
        "−1.5 个点",
    ),
    "fast_slow_weekly": Validation(
        "QQQ-Q5",
        1,
        "QQQ-Q5-r0002",
        "95% 平稳 bootstrap",
        "0.84",
        "+3.7 个点（−8.1..+7.7）",
        "+8.0%（−1.6..+16.5）",
        "−2.9 个点（−7.1..+1.2）",
        "−4.1 个点",
    ),
}
VALIDATION_ROWS = {
    "drawdown": "最大回撤浅了",
    "cvar": "CVaR 5% 相对改善",
    "geometric": "几何年化",
    "cost": "25 bps 下的几何年化",
}

# 回填检验汇报逐行 JSON 记录里的全精度数。
# 跑完逐项核对，只印不拦。
BACKFILL = {
    "fast_slow": {
        "comparison.cvar_5pct_relative_improvement": 0.06762813626462504,
        "comparison.max_drawdown": 0.19247427098472558,
        "comparison.geometric_annual_return": 0.013132650009683555,
        "cost.falsification": 0.006261356936889451,
        "events.protective": 26,
        "events.false_alarm": 18,
        "events.neutral": 102,
    },
    "trend4": {
        "comparison.cvar_5pct_relative_improvement": 0.06032950764785847,
        "comparison.max_drawdown": 0.2062102927772601,
        "comparison.geometric_annual_return": 0.010395077809789433,
        "cost.falsification": 0.005237110524457522,
        "events.protective": 18,
        "events.false_alarm": 13,
        "events.neutral": 101,
    },
    "slow_only": {
        "comparison.cvar_5pct_relative_improvement": -0.008393312442394343,
        "comparison.max_drawdown": 0.24354415926075046,
        "comparison.geometric_annual_return": 0.0170758127781534,
        "cost.falsification": 0.015438007360233152,
        "events.protective": 6,
        "events.false_alarm": 22,
        "events.neutral": 5,
    },
    "fast_slow_weekly": {
        "comparison.cvar_5pct_relative_improvement": 0.10421983318329192,
        "comparison.max_drawdown": 0.33642788573929694,
        "comparison.geometric_annual_return": 0.015348434071946526,
        "cost.falsification": 0.003122447268804729,
        "events.protective": 28,
        "events.false_alarm": 14,
        "events.neutral": 293,
    },
}

app = typer.Typer(help="日报方案决定表：只量开发窗口，不判、不计数、不改门、不写研究账本。")


@dataclass(frozen=True)
class Row:
    id: str
    kind: Literal["fixed", "overlay"]
    label: str
    function: str
    params: dict[str, Any] = field(default_factory=dict)
    rebalance: Rebalance = field(default_factory=Rebalance)


def table_rows(root_dir: Path) -> list[Row]:
    rows = [
        Row(
            f"fixed_{e:.1f}",
            "fixed",
            f"固定敞口 {e:.1f}",
            "fixed_exposure",
            {"exposure": e},
            FIXED_REBALANCE,
        )
        for e in FIXED_EXPOSURES
    ]
    configs = {config.id: config for config in bc.CONFIGS}
    for name, forward in OVERLAYS.items():
        config = configs[name]
        entry = bc.source_entry(root_dir, config)
        rows.append(
            Row(
                name,
                "overlay",
                f"{forward} {config.question} {config.strategy}",
                entry.function,
                dict(entry.params),
                entry.rebalance.to_rebalance(),
            )
        )
    return rows


def market_window(loaded: LoadedSettings, gate: tr.TrialGate, market: tr.Market) -> Window:
    return resolve_window(loaded, gate.markets[market].question_type_for(RESERVE), "development")


def run_spec(row: Row, market: tr.Market, gate: tr.TrialGate) -> RunSpec:
    return RunSpec(
        question_type=gate.markets[market].question_type_for(RESERVE),
        strategy_id=row.id,
        params=row.params,
        window_role="development",
        rebalance=row.rebalance,
        delay_days=0,
        leveraged_benchmarks=False,
        reserve=RESERVE,
    )


def _error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}".splitlines()[0]


def check_targets(
    rows: Sequence[Row],
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for market in MARKETS:
        for row in rows:
            found: dict[str, Any] = {"market": market, "row": row.id}
            try:
                window = market_window(loaded, gate, market)
                targets = strategy_targets(
                    tr.STRATEGY_FUNCTIONS[row.function], datasets[market].inputs, row.params
                )
                dates = pd.DatetimeIndex(targets.index)
                inside = targets.loc[
                    (dates >= pd.Timestamp(window.start))
                    & (True if window.end is None else dates <= pd.Timestamp(window.end))
                ]
                missing = int(inside.isna().sum())
                found.update(
                    window_id=window.window_id,
                    window_days=len(inside),
                    missing_days=missing,
                    ok=len(inside) > 0 and missing == 0,
                )
            except Exception as exc:
                found.update(ok=False, error=_error(exc))
            checks.append(found)
    return checks


def at_cost(
    data: BenchData,
    row: Row,
    window: Window,
    loaded: LoadedSettings,
    bps: float = COST_BPS,
) -> dict[str, Any]:
    targets = strategy_targets(tr.STRATEGY_FUNCTIONS[row.function], data.inputs, row.params)
    own = simulate(
        data.returns,
        targets,
        start=window.start,
        end=window.end,
        rebalance=row.rebalance,
        cost_bps=bps,
    )
    matched = simulate(
        data.returns,
        constant_targets(data.returns.index, float(own.exposure.mean())),
        start=window.start,
        end=window.end,
        rebalance=row.rebalance,
        cost_bps=bps,
    )
    cash = data.returns["cash"]
    strategy = summarize(own, cash, loaded)
    benchmark = summarize(matched, cash, loaded)
    return {
        "bps": bps,
        "strategy": strategy["geometric_annual_return"],
        "benchmark": benchmark["geometric_annual_return"],
        "difference": compare(strategy, benchmark, loaded)["geometric_annual_return"],
    }


def segment_tables(section: Mapping[str, Any] | None) -> dict[str, Any]:
    section = section or {}
    slow = section.get("slow_bear") or []
    bull = section.get("long_bull") or []
    measured_slow = [s for s in slow if s.get("difference") is not None]
    measured_bull = [s for s in bull if s.get("annual_difference") is not None]
    return {
        "slow_bear": slow,
        "long_bull": bull,
        "slow_bear_summary": {
            **(section.get("slow_bear_summary") or {}),
            "positive": sum(s["difference"] > 0 for s in measured_slow),
            "median_strategy_return": median(s["strategy_return"] for s in measured_slow)
            if measured_slow
            else None,
        },
        "long_bull_summary": {
            "count": len(measured_bull),
            "median_annual_difference": median(s["annual_difference"] for s in measured_bull)
            if measured_bull
            else None,
            "median_strategy_annual": median(s["strategy_annual"] for s in measured_bull)
            if measured_bull
            else None,
        },
    }


def evaluate(
    row: Row,
    market: tr.Market,
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
) -> dict[str, Any]:
    found: dict[str, Any] = {"row": row.id, "kind": row.kind, "market": market}
    try:
        window = market_window(loaded, gate, market)
        slow, bull = gate.markets[market].segment_lists(window.window_id)
        outcome = run_strategy(
            data,
            tr.STRATEGY_FUNCTIONS[row.function],
            run_spec(row, market, gate),
            loaded,
            falsification_confidence=gate.confidence,
            episodes=gate.markets[market].event_episodes,
            slow_bears=slow,
            long_bulls=bull,
        ).to_dict()
        events = bc._events(outcome.get("events"))
        cost = at_cost(data, row, window, loaded)
        cost["falsification"] = outcome["falsification"]["cost_25"]["geometric_annual_return"]
        found.update(
            status="ok",
            window=outcome["window"],
            strategy={key: outcome["strategy"][key] for key in METRICS},
            benchmark={key: outcome["benchmarks"]["matched_constant"][key] for key in METRICS},
            comparison={key: outcome["comparison"][key] for key in COMPARED},
            cost=cost,
            events={"count": events["count"], **events["outcomes"]},
            segments=segment_tables(outcome.get("segments")),
        )
    except BenchCheckError as exc:
        found.update(status="check_failed", problems=exc.problems)
    except Exception as exc:
        found.update(status="error", error=_error(exc))
    return dict(gp._clean(found))


def validation_column() -> dict[str, Any]:
    return {
        name: {
            "source": f"research/{item.question}/decision.md",
            **{key: value for key, value in asdict(item).items() if key != "question"},
        }
        for name, item in VALIDATION.items()
    }


def century_column(result: Mapping[str, Any] | None) -> dict[str, Any]:
    if result is None:
        return {"status": "pending"}
    if result["run_id"] != CENTURY_RUN_ID:
        raise ValueError(f"not a {CENTURY_RUN_ID} result")
    return {
        "status": "filled",
        "run_id": result["run_id"],
        "snapshot_id": result["snapshot_id"],
        "verdict": result["reading"]["verdict"],
        "reason": result["reading"]["reason"],
        "fidelity_holds": result["fidelity"]["holds"],
        "configs": {
            name: {"config": config, **result["sections"]["main"][config]["summary"]["slow_bear"]}
            for name, config in CENTURY.items()
        },
    }


def backfill_check(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_id = {row["row"]: row for row in rows if row["market"] == "us_market"}
    checks = []
    for name, expected in BACKFILL.items():
        row = by_id.get(name)
        differs = []
        if row is None or row.get("status") != "ok":
            differs.append("没有算出")
        else:
            for key, value in expected.items():
                section, metric = key.split(".")
                actual = (row.get(section) or {}).get(metric)
                if (
                    actual is None
                    or not math.isfinite(actual)
                    or abs(actual - value) > BACKFILL_TOLERANCE
                ):
                    differs.append(key)
        checks.append({"row": name, "consistent": not differs, "differs": differs})
    return checks


def summary_rows(
    rows: Sequence[Mapping[str, Any]],
    validation: Mapping[str, Any],
    century: Mapping[str, Any],
) -> list[dict[str, Any]]:
    by_cell = {(r["row"], r["market"]): r for r in rows}
    summary = []
    for name in dict.fromkeys(r["row"] for r in rows):
        cells: dict[str, Any] = {"row": name}
        for market in MARKETS:
            row = by_cell.get((name, market))
            if row is None or row["status"] != "ok":
                cells[market] = None
                continue
            overlay = row["kind"] == "overlay"
            slow = row["segments"]["slow_bear_summary"]
            bull = row["segments"]["long_bull_summary"]
            cells[market] = {
                "max_drawdown": row["strategy"]["max_drawdown"],
                "geometric": row["strategy"]["geometric_annual_return"],
                "max_drawdown_difference": row["comparison"]["max_drawdown"] if overlay else None,
                "geometric_difference": row["comparison"]["geometric_annual_return"]
                if overlay
                else None,
                "slow_bears": slow.get("count"),
                "slow_bear_median": slow.get(
                    "median_difference" if overlay else "median_strategy_return"
                ),
                "slow_bear_positive": slow["positive"] if overlay else None,
                "long_bull_median": bull[
                    "median_annual_difference" if overlay else "median_strategy_annual"
                ],
            }
        cells["validation"] = validation.get(name)
        cells["century"] = century.get("configs", {}).get(name)
        summary.append(cells)
    return summary


def tradeoff_line(cells: Mapping[str, Any]) -> str | None:
    us = cells.get("us_market")
    if (
        us is None
        or us.get("slow_bear_positive") is None
        or us.get("long_bull_median") is None
        or us.get("slow_bear_median") is None
    ):
        return None
    x, y = us["long_bull_median"], us["slow_bear_median"]
    return (
        f"{cells['row']}：1929–2010 每年在长牛里{'少' if x < 0 else '多'} {abs(x) * 100:.1f} 点，"
        f"慢熊里段内{'多' if y > 0 else '少'} {abs(y) * 100:.1f} 点（中位）"
    )


def run_probe(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    root_dir: Path,
    *,
    rows: Sequence[Row] | None = None,
    century: Mapping[str, Any] | None = None,
    provenance: Mapping[str, Any],
    progress: Callable[[str, str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    century_cells = century_column(century)
    rows = table_rows(root_dir) if rows is None else rows
    results = []
    for market in MARKETS:
        for row in rows:
            found = evaluate(row, market, datasets[market], loaded, gate)
            results.append(found)
            if progress is not None:
                progress(market, row.id, found)
    validation = validation_column()
    windows = {market: market_window(loaded, gate, market) for market in MARKETS}
    return dict(
        gp._clean(
            {
                "run_id": RUN_ID,
                "owner_decision": OWNER_DECISION,
                "data_trial": DATA_TRIAL,
                "gate_sha256": lg.chain_digest(
                    (root_dir / tr.TRIALS_DIR / "prereg.yaml").read_bytes()
                ),
                "sources": {market: dict(datasets[market].source) for market in MARKETS},
                "settings_sha256": loaded.sha256,
                "windows": {
                    market: {
                        "window_id": w.window_id,
                        "start": gp._iso(w.start),
                        "end": gp._iso(w.end),
                    }
                    for market, w in windows.items()
                },
                "provenance": dict(provenance),
                "rows": [{"id": row.id, "kind": row.kind, "label": row.label} for row in rows],
                "results": results,
                "validation": validation,
                "century": century_cells,
                "backfill": backfill_check(results),
                "summary": summary_rows(results, validation, century_cells),
                "complete": all(row["status"] == "ok" for row in results),
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    )


def _pct(value: Any) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def _points(value: Any) -> str:
    return "—" if value is None else f"{value * 100:+.1f}"


def _relative(value: Any) -> str:
    return "—" if value is None else f"{value * 100:+.1f}%"


def _with_difference(value: Any, difference: Any) -> str:
    text = _pct(value)
    return text if difference is None else f"{text}（{_points(difference)}）"


def _table_row(cells: Sequence[Any]) -> str:
    return "| " + " | ".join("—" if cell is None else str(cell) for cell in cells) + " |"


def _market_cell(cell: Mapping[str, Any] | None) -> str:
    if cell is None:
        return "算不出"
    positive = cell["slow_bear_positive"]
    counts = "" if positive is None else f"（{positive}/{cell['slow_bears']}）"
    return (
        f"{_with_difference(cell['max_drawdown'], cell['max_drawdown_difference'])} / "
        f"{_with_difference(cell['geometric'], cell['geometric_difference'])} / "
        f"{_points(cell['slow_bear_median'])}{counts} / {_points(cell['long_bull_median'])}"
    )


def _render_market(
    result: Mapping[str, Any],
    market: str,
    labels: Mapping[str, str],
) -> list[str]:
    window = result["windows"][market]
    title = MARKET_TEXT[market]
    rows = [r for r in result["results"] if r["market"] == market]
    mismatches = {
        r["row"]: "、".join(r["differs"]) for r in result["backfill"] if not r["consistent"]
    }
    lines = [
        "",
        f"## {title}（开发窗口 {window['window_id']}，{window['start']}..{window['end']}）",
        "",
        "覆盖行括号里是相对同敞口固定敞口（台子的主对照）的比较：几何年化差（点）、"
        "最大回撤浅了几个点、最差 5% 日均的相对改善（正为更温和）。",
        "",
        "| 方案 | 几何年化 | 25 bps 下几何年化 | 最大回撤 | 最差 5% 日均 | 平均敞口 | 年换手 | "
        "减仓事件（保护 / 误报 / 中性） |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        name = row["row"]
        label = labels.get(name, name)
        if row["status"] != "ok":
            lines.append(_table_row([label, f"算不出：{row.get('error', row['status'])}"]))
            continue
        overlay = row["kind"] == "overlay"
        if overlay and market == "us_market" and name in mismatches:
            label += f"（和回填检验不一致：{mismatches[name]}）"
        own, cost = row["strategy"], row["cost"]
        comp = row["comparison"] if overlay else {}
        cvar = "—" if own["cvar_5pct"] is None else f"{own['cvar_5pct'] * 100:.2f}%"
        if overlay:
            cvar += f"（{_relative(comp['cvar_5pct_relative_improvement'])}）"
        events = row["events"]
        lines.append(
            _table_row(
                [
                    label,
                    _with_difference(
                        own["geometric_annual_return"], comp.get("geometric_annual_return")
                    ),
                    _with_difference(cost["strategy"], cost["difference"] if overlay else None),
                    _with_difference(own["max_drawdown"], comp.get("max_drawdown")),
                    cvar,
                    "—" if own["average_exposure"] is None else f"{own['average_exposure']:.2f}",
                    "—" if own["annual_turnover"] is None else f"{own['annual_turnover']:.2f}",
                    " / ".join(
                        "—" if events[k] is None else str(events[k])
                        for k in ("protective", "false_alarm", "neutral")
                    )
                    if overlay
                    else "—",
                ]
            )
        )
    good = [r for r in rows if r["status"] == "ok"]
    lines += [
        "",
        f"### 慢熊段（{title}，门里登记的段；A 方案、B 同敞口固定敞口，段内收益，点）",
        "",
        "| 方案 | 段 | 起 | 止 | A | B | D |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in good:
        overlay = row["kind"] == "overlay"
        for s in row["segments"]["slow_bear"]:
            lines.append(
                _table_row(
                    [
                        labels.get(row["row"], row["row"]),
                        s["id"],
                        s["start"],
                        s["end"],
                        _points(s["strategy_return"]),
                        _points(s["benchmark_return"]) if overlay else "—",
                        _points(s["difference"]) if overlay else "—",
                    ]
                )
            )
    lines += [
        "",
        "| 方案 | 慢熊段数 | D 中位（固定敞口：段内收益中位） | D > 0 段数 | 最小 D |",
        "|---|---|---|---|---|",
    ]
    for row in good:
        overlay = row["kind"] == "overlay"
        slow = row["segments"]["slow_bear_summary"]
        lines.append(
            _table_row(
                [
                    labels.get(row["row"], row["row"]),
                    slow.get("count"),
                    _points(slow.get("median_difference" if overlay else "median_strategy_return")),
                    slow["positive"] if overlay else "—",
                    _points(slow.get("min_difference")) if overlay else "—",
                ]
            )
        )
    lines += [
        "",
        f"### 长牛段（{title}，年化几何，点/年）",
        "",
        "| 方案 | 段 | 起 | 止 | A | 差 |",
        "|---|---|---|---|---|---|",
    ]
    for row in good:
        for s in row["segments"]["long_bull"]:
            lines.append(
                _table_row(
                    [
                        labels.get(row["row"], row["row"]),
                        s["id"],
                        s["start"],
                        s["end"],
                        _points(s["strategy_annual"]),
                        _points(s["annual_difference"]) if row["kind"] == "overlay" else "—",
                    ]
                )
            )
    lines += [
        "",
        "| 方案 | 长牛段数 | 中位（覆盖：年化差；固定敞口：年化几何） |",
        "|---|---|---|",
    ]
    for row in good:
        bull = row["segments"]["long_bull_summary"]
        lines.append(
            _table_row(
                [
                    labels.get(row["row"], row["row"]),
                    bull["count"],
                    _points(
                        bull[
                            "median_annual_difference"
                            if row["kind"] == "overlay"
                            else "median_strategy_annual"
                        ]
                    ),
                ]
            )
        )
    return lines


def render_markdown(result: Mapping[str, Any]) -> str:
    labels = {row["id"]: row["label"] for row in result["rows"]}
    kinds = {row["id"]: row["kind"] for row in result["rows"]}
    century = result["century"]
    pending = century["status"] == "pending"
    sources = result["sources"]
    lines = [
        "# 日报方案决定表：固定敞口和趋势覆盖",
        "",
        f"- 运行：{result['run_id']}；依据 {result['owner_decision']}；"
        "描述表，不判、不计数、不改门、不写研究账本，不读验证和一致性窗口。",
        f"- 提交 {result['provenance']['commit']}；数据：计划 {result['data_trial']} 的快照"
        f"（美股大盘 {sources['us_market']['signal_snapshot_id']}，"
        f"QQQ {sources['qqq']['snapshot_id']}），"
        f"载入后截到 2010-12-31；段清单取自门文件（sha256 {result['gate_sha256']}）。",
        "- 固定敞口三行和四个覆盖都月底调仓（fast_slow_weekly 周末调仓）；"
        "覆盖照回填检验的配置原样取。",
    ]
    mismatches = [item for item in result["backfill"] if not item["consistent"]]
    if mismatches:
        for item in mismatches:
            lines.append(
                f"- {labels.get(item['row'], item['row'])}：和回填检验不一致："
                + "、".join(item["differs"])
            )
    else:
        lines.append(f"- 四个覆盖在 1929–2010 和回填检验（{BACKFILL_RUN_ID}）一致。")
    lines += [
        "",
        "## 汇总",
        "",
        "| 方案 | 1929–2010：回撤 / 几何 / 慢熊 D 中位（正段数）/ 长牛代价 | 1999–2010：同 | "
        "2011–2021（抄记录，相对同敞口固定敞口）：左尾 / 几何 | "
        "1871–1928（乙，月频等价）：D 中位 / 正占比 |",
        "|---|---|---|---|---|",
    ]
    for cells in result["summary"]:
        name = cells["row"]
        val = cells["validation"]
        old = cells["century"]
        validation_text = (
            "无记录"
            if val is None
            else (f"{val['cvar']} / {val['geometric']}（`{val['run_id']}`）")
        )
        century_text = (
            "待乙"
            if pending
            else (
                ("没有月频等价" if kinds.get(name) == "overlay" else "无")
                if old is None
                else f"{_points(old['median_difference'])} / {_pct(old['positive_share'])}"
            )
        )
        lines.append(
            _table_row(
                [
                    labels.get(name, name),
                    _market_cell(cells["us_market"]),
                    _market_cell(cells["qqq"]),
                    validation_text,
                    century_text,
                ]
            )
        )
    lines += [
        "",
        "- 固定敞口行：回撤、几何是绝对数，慢熊格是段内收益中位，长牛格是长牛段年化几何中位。"
        "覆盖行：括号里是相对主对照（回撤浅了几个点、几何年化差），"
        "慢熊格是 D 中位（D > 0 段数/段数），长牛格是年化差中位。",
    ]
    for cells in result["summary"]:
        text = tradeoff_line(cells)
        if text is not None:
            name = cells["row"]
            lines.append(f"- {labels.get(name, name)}{text[len(name) :]}")
    for market in MARKETS:
        lines += _render_market(result, market, labels)
    lines += ["", f"### 和回填检验（{BACKFILL_RUN_ID}）核对，只印", ""]
    for item in result["backfill"]:
        text = "一致" if item["consistent"] else "不一致：" + "、".join(item["differs"])
        lines.append(f"- {labels.get(item['row'], item['row'])}：{text}")
    lines += [
        "",
        "## 2011–2021 验证窗口（只抄已发布的 decision.md，相对同敞口固定敞口）",
        "",
        "| 方案 | 记录 | 区间 | 平均敞口 | 回撤浅了 | CVaR 5% 相对改善 | 几何年化差 | "
        "25 bps 下几何年化差 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in result["rows"]:
        name = row["id"]
        val = result["validation"].get(name)
        if val is None:
            cells = [labels.get(name, name), "无记录", *(["—"] * 6)]
        else:
            cells = [
                labels.get(name, name),
                f"`{val['run_id']}`（{val['source']}）",
                *[
                    val[k]
                    for k in ("interval", "exposure", "drawdown", "cvar", "geometric", "cost")
                ],
            ]
        lines.append(_table_row(cells))
    lines += ["", "## 1871–1928（乙的探针，月频等价，不是同一配置）", ""]
    if pending:
        lines.append("待乙：跑这张表时没有给乙的结果文件。")
    else:
        reading = {
            "holds": "趋势覆盖的慢熊效果在 1871–1928 样本外成立",
            "fails": "1871–1928 没站住",
            "unread": "只印不读",
        }[century["verdict"]]
        reasons = {"few_slow_bears": "慢熊段少于 4 段", "fidelity": "保真核对不成立"}
        reason = century.get("reason")
        suffix = f"（{reasons.get(reason, reason)}）" if reason else ""
        lines += [
            f"- 来源 {century['run_id']}，快照 {century['snapshot_id']}；"
            f"乙的读法：{reading}{suffix}。",
            "",
            "| 方案 | 月频等价 | 慢熊段数 | D 中位 | D > 0 段数 | D > 0 占比 |",
            "|---|---|---|---|---|---|",
        ]
        for row in result["rows"]:
            old = century["configs"].get(row["id"])
            if old is None:
                cells = [
                    row["label"],
                    "没有月频等价" if row["kind"] == "overlay" else "无",
                    *(["—"] * 4),
                ]
            else:
                cells = [
                    row["label"],
                    old["config"],
                    old["count"],
                    _points(old["median_difference"]),
                    old["positive"],
                    _pct(old["positive_share"]),
                ]
            lines.append(_table_row(cells))
    return "\n".join([*lines, "", "production_effect=none；broker_action=none。", ""])


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir.joinpath(RESULT_JSON).write_text(
        json.dumps(gp._clean(dict(result)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    out_dir.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


def load_data(
    snapshot_root: Path,
) -> tuple[dict[str, BenchData], tr.TrialGate, LoadedSettings, Path]:
    loaded = load_settings()
    root_dir = lg.research_root(loaded)
    gate, plan = tr.load_trial(root_dir, DATA_TRIAL)
    problems = tr.gate_problems(gate, loaded)
    if problems:
        raise ValueError("; ".join(problems))
    datasets: dict[str, BenchData] = {}
    for market in MARKETS:
        data = tr.market_data(
            market, gate, plan, snapshot_root=snapshot_root, project_root=PROJECT_ROOT
        )
        problems = tr._data_problems(market, gate, plan, data)
        if problems:
            raise ValueError(f"{market}: " + "; ".join(problems))
        datasets[market] = gp.cut(data)
    return datasets, gate, loaded, root_dir


def read_century(path: Path) -> dict[str, Any]:
    result = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(result, dict):
        raise ValueError(f"not a {CENTURY_RUN_ID} result")
    century_column(result)
    return result


def _load(root: Path) -> tuple[dict[str, BenchData], tr.TrialGate, LoadedSettings, Path]:
    try:
        return load_data(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"policy_table: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None


def _load_rows(root_dir: Path) -> list[Row]:
    try:
        return table_rows(root_dir)
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"policy_table: 配置载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None


def _print_checks(checks: Sequence[Mapping[str, Any]]) -> bool:
    for row in checks:
        prefix = f"policy_table: {row['market']} {row['row']}"
        if "error" in row:
            typer.echo(f"{prefix} 算不出：{row['error']}")
        else:
            typer.echo(
                f"{prefix} 窗口 {row['window_id']} {row['window_days']} 天，"
                f"缺 {row['missing_days']} 天，{'可以' if row['ok'] else '不行'}"
            )
    return all(row["ok"] for row in checks)


@app.callback()
def main() -> None:
    """描述固定敞口和趋势覆盖，不判、不改门。"""


@app.command("check")
def check_command(
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    datasets, gate, loaded, root_dir = _load(root)
    rows = _load_rows(root_dir)
    if not _print_checks(check_targets(rows, datasets, loaded, gate)):
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，保存 JSON 和中文报告。")],
    century: Annotated[Path | None, typer.Option(help="乙的 century_trend_result.json。")] = None,
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("policy_table: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    old = None
    if century is not None:
        try:
            old = read_century(century)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            typer.echo(f"policy_table: 乙的结果不能用：{str(exc).splitlines()[0]}")
            raise typer.Exit(code=1) from None
    datasets, gate, loaded, root_dir = _load(root)
    rows = _load_rows(root_dir)
    if not _print_checks(check_targets(rows, datasets, loaded, gate)):
        typer.echo("policy_table: 开跑前检查没过，停下。")
        raise typer.Exit(code=1)

    def progress(market: str, row_id: str, found: dict[str, Any]) -> None:
        typer.echo(f"policy_table: {market} {row_id} {found['status']}")

    result = run_probe(
        datasets,
        loaded,
        gate,
        root_dir,
        rows=rows,
        century=old,
        provenance=git_provenance(PROJECT_ROOT),
        progress=progress,
    )
    write_result(result, out)
    for item in result["backfill"]:
        text = "一致" if item["consistent"] else "不一致：" + "、".join(item["differs"])
        typer.echo(f"policy_table: 回填检验核对 {item['row']} {text}")
    for cells in result["summary"]:
        tradeoff = tradeoff_line(cells)
        if tradeoff is not None:
            typer.echo(f"policy_table: {tradeoff}")
    status = "完成" if result["complete"] else "有配置出错"
    old_status = "待乙" if result["century"]["status"] == "pending" else "已填"
    typer.echo(f"policy_table: {status}；1871–1928 一列{old_status}；结果目录 {out}")


if __name__ == "__main__":
    app()
