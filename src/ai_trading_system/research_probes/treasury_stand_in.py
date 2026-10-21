"""Check the Treasury stand-in before T0008, without changing the research gate.

Decision: owner_decision:TRADING-2565:2026-10-21:treasury_1929. T0008 reads 1929–2010
with treasury_10y_shiller. First compare it with treasury_10y on us_market_1962_2010:
60/40 mixes under the portfolio ruler, 15 declines, monthly and calendar-year reserve
returns, agreement in reserve-excess direction, and Shiller rate shape before 1953
and from 1962 (statistics only, never print yield observations).

The use rule is fixed before any run: calendar-year correlation below 0.90 or fewer
than 12 of 15 agreeing declines means diverges. A failed T0008 then indicates a
process or measurement problem (proxy and DGS10 disagree), not an absent mechanism.
Monthly averaging smears a month's move over two months: monthly return correlation
is printed, never judged (about 0.6 for random-walk yields). A same-basis comparison
prices DGS10 monthly means with the same monthly formula; it too is printed only.
From 1953 the Shiller and DGS10 monthly-mean rates should be the same series.

Read-only: no trial records, ledger writes, gate changes, or post-2010 observations.
production_effect=none; broker_action=none.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import (
    SHILLER_TREASURY_RESERVE,
    SHILLER_TREASURY_SOURCE,
    TREASURY_RESERVE,
    TREASURY_SOURCE,
    BenchData,
    par_bond_price,
)
from ai_trading_system.research_bench.fixed_strategy import fixed_exposure
from ai_trading_system.research_bench.run import RunSpec, run_strategy
from ai_trading_system.research_bench.settings import (
    EpisodeSpec,
    LoadedSettings,
    Window,
    load_settings,
    resolve_window,
)
from ai_trading_system.research_bench.simulate import Rebalance
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_probes import gate_power as gp

RUN_ID = "TRADING-2565-40"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-21:treasury_1929"
PLAN_ID = "T0008"
MARKET: tr.Market = "us_market"
QUESTION_TYPE = "us_market_bond_trial"
WEIGHT = 0.6
REBALANCE = Rebalance(schedule="month_end")
RESERVES = (TREASURY_RESERVE, SHILLER_TREASURY_RESERVE)
# Shape periods of Shiller's rate: before GS10 (1953) inside the trial window, and the DGS10 years.
PERIODS = {"1929_1952": ("1929-01", "1952-12"), "1962_2010": ("1962-01", "2010-12")}
# Second differences within half a rounding step of Shiller's two decimals count as flat.
FLAT_STEP = 0.011
# The use rule, fixed before any run.
MIN_CORRELATION = 0.90
MIN_AGREE = 12
RESULT_JSON = "treasury_stand_in.json"
app = typer.Typer(help="合成国债核对：1962–2010 上 DGS10 和 Shiller 两种国债并排，只读。")


def cut(data: BenchData, cutoff: str = "2010-12-31") -> BenchData:
    mask = data.returns.index <= pd.Timestamp(cutoff)
    return replace(
        data,
        returns=data.returns.loc[mask],
        inputs=data.inputs.loc[data.inputs.index <= pd.Timestamp(cutoff)],
        reserves={name: series.loc[mask] for name, series in data.reserves.items()},
    )


def period_returns(daily: pd.Series, window: Window, freq: str) -> pd.Series:
    mask = daily.index >= pd.Timestamp(window.start)
    if window.end is not None:
        mask &= daily.index <= pd.Timestamp(window.end)
    inside = daily.loc[mask]
    periods = pd.DatetimeIndex(inside.index).to_period(freq)
    return (1 + inside).groupby(periods).prod() - 1


def shape(yields: pd.Series) -> dict[str, dict[str, Any]]:
    rates = pd.Series(yields.to_numpy(), index=pd.DatetimeIndex(yields.index).to_period("M"))
    rates = rates.sort_index()
    out = {}
    for name, (start, end) in PERIODS.items():
        inside = rates.loc[start:end].dropna()
        second = inside.diff().diff().dropna()
        out[name] = {
            "months": len(inside),
            "change_std": float(inside.diff().std()),
            "flat_share": float((second.abs() <= FLAT_STEP).mean()),
        }
    return out


def month_means(daily: pd.Series) -> pd.Series:
    observations = daily.dropna()
    return observations.groupby(pd.DatetimeIndex(observations.index).to_period("M")).mean()


def monthly_bond_returns(rates: pd.Series) -> pd.Series:
    decimal = rates / 100
    previous = decimal.reindex(decimal.index - 1).to_numpy(dtype=float)
    monthly = par_bond_price(previous, decimal.to_numpy(dtype=float)) - 1 + previous / 12
    return pd.Series(monthly, index=rates.index)


def same_basis(shiller: pd.Series, dgs10: pd.Series, window: Window) -> dict[str, Any]:
    observations = shiller.dropna()
    rates = pd.Series(
        observations.to_numpy(), index=pd.DatetimeIndex(observations.index).to_period("M")
    )
    means = month_means(dgs10)
    start = pd.Period(window.start, freq="M")
    end = pd.Period(window.end if window.end is not None else "2010-12", freq="M")
    priced = (
        pd.DataFrame({"shiller": monthly_bond_returns(rates), "dgs10": monthly_bond_returns(means)})
        .loc[start:end]
        .dropna()
    )
    gap = (rates - means).abs().loc[start:end].dropna()
    return {
        "months": len(priced),
        "correlation": float(priced["shiller"].corr(priced["dgs10"])),
        "yield_gap_mean": float(gap.mean()),
        "yield_gap_max": float(gap.max()),
    }


def use_rule(annual_correlation: float, agree: int) -> str:
    return "usable" if annual_correlation >= MIN_CORRELATION and agree >= MIN_AGREE else "diverges"


def stand_in(
    data: BenchData,
    loaded: LoadedSettings,
    window: Window,
    declines: Sequence[EpisodeSpec],
    yields: pd.Series,
    dgs10_yields: pd.Series,
) -> dict[str, Any]:
    portfolios = {}
    for reserve in RESERVES:
        spec = RunSpec(
            question_type=QUESTION_TYPE,
            strategy_id=f"mix60_{reserve}",
            params={"exposure": WEIGHT},
            window_role="development",
            rebalance=REBALANCE,
            reserve=reserve,
        )
        portfolio = run_strategy(
            data, fixed_exposure, spec, loaded, declines=declines, portfolio_weight=WEIGHT
        ).portfolio
        # Supplying portfolio_weight requires a portfolio section in every successful run.
        assert portfolio is not None
        portfolios[reserve] = portfolio
    tables = {
        freq: pd.DataFrame(
            {reserve: period_returns(data.reserves[reserve], window, freq) for reserve in RESERVES}
        )
        for freq in ("M", "Y")
    }
    correlations = {
        freq: float(table[TREASURY_RESERVE].corr(table[SHILLER_TREASURY_RESERVE]))
        for freq, table in tables.items()
    }
    rows = []
    for dgs10_segment, shiller_segment in zip(
        portfolios[TREASURY_RESERVE]["segments"],
        portfolios[SHILLER_TREASURY_RESERVE]["segments"],
        strict=True,
    ):
        rows.append(
            {
                "id": dgs10_segment["id"],
                "months": dgs10_segment["months"],
                "reserve_excess": {
                    TREASURY_RESERVE: dgs10_segment["reserve_excess"],
                    SHILLER_TREASURY_RESERVE: shiller_segment["reserve_excess"],
                },
                "ratio": {
                    TREASURY_RESERVE: dgs10_segment["ratio"],
                    SHILLER_TREASURY_RESERVE: shiller_segment["ratio"],
                },
                "agree": bool(
                    np.sign(dgs10_segment["reserve_excess"])
                    == np.sign(shiller_segment["reserve_excess"])
                ),
            }
        )
    agree = sum(row["agree"] for row in rows)
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "window": {
            "window_id": window.window_id,
            "start": str(window.start),
            "end": str(window.end) if window.end is not None else None,
        },
        "weight": WEIGHT,
        "portfolio": portfolios,
        "monthly": {
            "months": len(tables["M"]),
            "correlation": correlations["M"],
            "volatility": {
                reserve: float(tables["M"][reserve].std() * math.sqrt(12)) for reserve in RESERVES
            },
        },
        "annual": {"years": len(tables["Y"]), "correlation": correlations["Y"]},
        "same_basis": same_basis(yields, dgs10_yields, window),
        "declines": rows,
        "agree": agree,
        "rate_shape": shape(yields),
        "rule": {"min_correlation": MIN_CORRELATION, "min_agree": MIN_AGREE},
        "verdict": use_rule(correlations["Y"], agree),
        "production_effect": "none",
        "broker_action": "none",
    }


def load_inputs(
    snapshot_root: Path,
) -> tuple[BenchData, LoadedSettings, Window, tuple[EpisodeSpec, ...], pd.Series, pd.Series]:
    loaded = load_settings()
    gate, plan = tr.load_trial(lg.research_root(loaded), PLAN_ID)
    problems = tr.gate_problems(gate, loaded)
    if problems:
        raise ValueError("; ".join(problems))
    window = resolve_window(loaded, QUESTION_TYPE, "development")
    declines = gate.markets[MARKET].decline_list(window.window_id)
    if not declines:
        raise ValueError(f"the gate lists no declines for {window.window_id}")
    data = tr.market_data(
        MARKET, gate, plan, snapshot_root=snapshot_root, project_root=PROJECT_ROOT
    )
    missing = [reserve for reserve in RESERVES if reserve not in data.reserves]
    if missing:
        raise ValueError(f"data have no {', '.join(missing)}")
    frames = signal_sources.load_snapshot(PROJECT_ROOT, plan.data.signal_snapshot_id).frames
    shiller = frames[SHILLER_TREASURY_SOURCE]["value"]
    dgs10 = frames[TREASURY_SOURCE]["value"]
    return (
        cut(data),
        loaded,
        window,
        declines,
        shiller.loc[shiller.index <= pd.Timestamp("2010-12-31")],
        dgs10.loc[dgs10.index <= pd.Timestamp("2010-12-31")],
    )


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir.joinpath(RESULT_JSON).write_text(
        json.dumps(gp._clean(dict(result)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _pct(value: Any) -> str:
    return "—" if value is None else f"{value * 100:+.2f}%"


def print_summary(result: Mapping[str, Any]) -> None:
    typer.echo(
        f"treasury_stand_in: {result['window']['window_id']} 60/40，"
        f"下跌段 {len(result['declines'])} 段"
    )
    for reserve in RESERVES:
        section = result["portfolio"][reserve]
        typer.echo(
            f"treasury_stand_in: {reserve} 储备几何 {_pct(section['legs']['reserve'])}，"
            f"对现金 {_pct(section['excess_annual'])}，"
            f"几何差 {_pct(section['geometric_difference'])}，"
            f"储备自身 {_pct(section['reserve_excess'])}，"
            f"再平衡 {_pct(section['rebalance_return'])}，"
            f"平时超额 {_pct(section['quiet_excess'])}；计入 {section['segment_count']} 段，"
            f"r 中位 {_pct(section['median_ratio'])}，"
            f"r>0 占比 {_pct(section['positive_share'])}，最小 r {_pct(section['min_ratio'])}"
        )
    for row in result["declines"]:
        excess, ratios = row["reserve_excess"], row["ratio"]
        typer.echo(
            f"treasury_stand_in: {row['id']} {row['months']} 个月 "
            f"储备超额 DGS10 {_pct(excess[TREASURY_RESERVE])} / "
            f"Shiller {_pct(excess[SHILLER_TREASURY_RESERVE])}，"
            f"r {_pct(ratios[TREASURY_RESERVE])} / {_pct(ratios[SHILLER_TREASURY_RESERVE])}"
            + ("" if row["agree"] else "，方向不同")
        )
    monthly, annual, basis = result["monthly"], result["annual"], result["same_basis"]
    volatility = monthly["volatility"]
    typer.echo(
        f"treasury_stand_in: 月收益 {monthly['months']} 个月，相关 {monthly['correlation']:.3f}，"
        f"年化波动 DGS10 {_pct(volatility[TREASURY_RESERVE])} / "
        f"Shiller {_pct(volatility[SHILLER_TREASURY_RESERVE])}"
    )
    typer.echo(
        f"treasury_stand_in: 日历年收益 {annual['years']} 年，相关 {annual['correlation']:.3f}"
    )
    typer.echo(
        f"treasury_stand_in: 同口径（DGS10 月均按同一公式）{basis['months']} 个月，"
        f"月收益相关 {basis['correlation']:.3f}；利率差平均 {basis['yield_gap_mean']:.3f}、"
        f"最大 {basis['yield_gap_max']:.3f} 个百分点（只印不判）"
    )
    for name, values in result["rate_shape"].items():
        typer.echo(
            f"treasury_stand_in: Shiller 利率 {name} {values['months']} 个月，"
            f"月变化标准差 {values['change_std']:.3f} 个百分点，"
            f"二阶差分近零占比 {_pct(values['flat_share'])}"
        )
    typer.echo(
        f"treasury_stand_in: 方向一致 {result['agree']}/{len(result['declines'])} 段；"
        f"判定 {result['verdict']}"
    )


@app.callback()
def main() -> None:
    """只核对合成国债，不改门、不记账。"""


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option(help="结果目录。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if out.joinpath(RESULT_JSON).exists():
        typer.echo("treasury_stand_in: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        inputs = load_inputs(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"treasury_stand_in: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None
    result = stand_in(*inputs)
    write_result(result, out)
    print_summary(result)
    typer.echo(f"treasury_stand_in: 结果目录 {out}")


if __name__ == "__main__":
    app()
