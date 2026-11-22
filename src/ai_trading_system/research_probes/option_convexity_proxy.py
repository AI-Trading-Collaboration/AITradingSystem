"""Pure-QQQ positive-convexity option proxy development probe (TRADING-2574).

Zero-cost development diagnostic on pure QQQ (2001-02-28..2010-12-31, mechanically cut
at 2010-12-31, zero 2011+ data read). Complements TRADING-2567-41 (2009+ Nasdaq option
indices BXN/NQTRI/NQCLRI) by covering:
1. Rolling long-Call stock replacement (3M/6M ATM Call and 6M 85% ITM Call + T-bills),
   including self-funded notional scaling up to buy-and-hold QQQ's max drawdown.
2. 2001-2002 dot-com bear tail and 2008 global financial crisis (missing from 2009+
   Nasdaq option indices), plus 90% Protective Put and 95-110 Collar proxies.

Pricing caveats (explicitly disclosed; not tradable execution):
- Dividend-free Black-Scholes priced with sigma_scale * VXN (sigma_scale in {0.9, 1.0, 1.1},
  where 1.1 models buyer ask-spread and skew penalty), no bid-ask spread, no skew,
  no early exercise, no commission.
- Print-only development diagnostic: does not enter trial ledgers, alter gates/rulers,
  touch daily reports, or authorize production/broker actions.
  production_effect=none; broker_action=none.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal, NamedTuple

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData, from_snapshot
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR
from ai_trading_system.research_bench.settings import LoadedSettings, load_settings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import asym_vxn_leverage as avl
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import leverage_table as lt
from ai_trading_system.research_probes import option_proxy as op

TASK_ID = "TRADING-2574_QQQ_OPTION_CONVEXITY_PROXY"
REQUIREMENT = "docs/requirements/TRADING-2574_QQQ_Option_Convexity_Proxy_Dev_Probe.md"
DATA_TRIAL = lt.DATA_TRIAL
TRADING_DAYS = 252

# All parameters below are the temporary development-diagnostic baseline documented in
# docs/requirements/TRADING-2574_QQQ_Option_Convexity_Proxy_Dev_Probe.md; exit upon owner
# review of the diagnostic verdict (not production or promotion thresholds).
DEV_START = date(2001, 2, 28)
DEV_END = date(2010, 12, 31)
SIGMA_SCALES: tuple[float, ...] = (0.9, 1.0, 1.1)
BASE_SIGMA_SCALE = 1.0
PESSIMISTIC_SIGMA_SCALE = 1.1
MARGIN_SPREAD = 0.01
NOTIONAL_GRID: tuple[float, ...] = tuple(round(1.0 + 0.05 * i, 2) for i in range(41))
KILL_MIN_EXCESS_OVER_HOLD = 0.010
KILL_MAX_PESSIMISTIC_GAP_VS_1X = 0.005

SUBPERIODS = avl.SUBPERIODS

CHECK_JSON = "option_convexity_proxy_check.json"
RESULT_JSON = "option_convexity_proxy.json"
RESULT_MD = "option_convexity_proxy.md"

StructureKind = Literal["call_replace", "protective_put", "collar"]


@dataclass(frozen=True)
class StructureSpec:
    structure_id: str
    label: str
    kind: StructureKind
    step_months: int
    strike_primary: float
    strike_short_call: float | None = None


def default_structures() -> tuple[StructureSpec, ...]:
    return (
        StructureSpec(
            "call_3m_atm",
            "现金 + 滚动 3M ATM Call (K=1.00)",
            "call_replace",
            step_months=3,
            strike_primary=1.00,
        ),
        StructureSpec(
            "call_6m_atm",
            "现金 + 滚动 6M ATM Call (K=1.00)",
            "call_replace",
            step_months=6,
            strike_primary=1.00,
        ),
        StructureSpec(
            "call_6m_itm85",
            "现金 + 滚动 6M 85% ITM Call (K=0.85)",
            "call_replace",
            step_months=6,
            strike_primary=0.85,
        ),
        StructureSpec(
            "put90_3m",
            "QQQ + 滚动 3M 90% 保护 Put (NQTRI 代理)",
            "protective_put",
            step_months=3,
            strike_primary=0.90,
        ),
        StructureSpec(
            "collar_95_110_3m",
            "QQQ + 滚动 3M 95/110 Collar (NQCLRI 代理)",
            "collar",
            step_months=3,
            strike_primary=0.95,
            strike_short_call=1.10,
        ),
    )


@dataclass(frozen=True)
class ConvexityInputs:
    qqq_return: pd.Series
    cash_return: pd.Series
    vxn: pd.Series
    rate: pd.Series
    filled_days: dict[str, int]
    max_loaded_date: date

    @classmethod
    def from_bench(cls, data: BenchData) -> ConvexityInputs:
        cut = gp.cut(data)
        index = pd.DatetimeIndex(cut.returns.index)
        max_date = index.max().date()
        keep = (index >= pd.Timestamp(op.DEV_START)) & (index <= pd.Timestamp(DEV_END))
        filled: dict[str, int] = {}
        series_map: dict[str, pd.Series] = {}
        for name, raw in (("vxn", cut.inputs["vxn"]), ("rate", cut.inputs["cash_rate_known"])):
            part = raw.loc[keep].astype(float)
            missing = int(part.isna().sum())
            part = part.ffill(limit=op.MAX_FILL_DAYS)
            if part.isna().any():
                raise ValueError(f"{name} has gaps longer than {op.MAX_FILL_DAYS} days")
            filled[name] = missing
            series_map[name] = part
        qqq = cut.returns.loc[keep, "qqq"].astype(float)
        cash = cut.returns.loc[keep, "cash"].astype(float)
        if not np.isfinite(qqq.to_numpy()).all() or not np.isfinite(cash.to_numpy()).all():
            raise ValueError("qqq or cash return has missing or nonfinite values")
        if (series_map["vxn"].to_numpy() <= 0.0).any():
            raise ValueError("vxn must be positive")
        rvals = series_map["rate"].to_numpy()
        if rvals.min() < op.RATE_RANGE[0] or rvals.max() > op.RATE_RANGE[1]:
            raise ValueError(f"rate outside {op.RATE_RANGE}")
        return cls(
            qqq_return=qqq,
            cash_return=cash,
            vxn=series_map["vxn"],
            rate=series_map["rate"],
            filled_days=filled,
            max_loaded_date=max_date,
        )


class CycleRecord(NamedTuple):
    start: str
    end: str
    days: int
    vxn_start: float
    net_premium_pct: float
    realized_vol_pct: float
    iv_minus_rv: float
    cycle_return: float


class StructurePath(NamedTuple):
    returns: pd.Series
    exposure: pd.Series
    start: pd.Timestamp
    cycles: tuple[CycleRecord, ...]
    ever_borrowed: bool
    bust_date: str | None


app = typer.Typer(help="TRADING-2574：纯 QQQ 期权正凸性结构（VXN+BS 代理）开发窗口诊断。")


def multi_month_bounds(index: pd.DatetimeIndex, step_months: int) -> list[tuple[int, int]]:
    """Consecutive multi-month rolls anchored at month-ends, with exact stub to the final date."""

    if step_months < 1:
        raise ValueError("step_months must be >= 1")
    month_ends = [i for i in range(len(index) - 1) if index[i].month != index[i + 1].month]
    if op._near_month_end(index[-1]):
        month_ends.append(len(index) - 1)
    if len(month_ends) < 2:
        raise ValueError("insufficient month-ends for option cycle")
    rolls = list(month_ends[::step_months])
    if rolls[-1] != month_ends[-1]:
        rolls.append(month_ends[-1])
    return list(zip(rolls[:-1], rolls[1:], strict=True))


def _delta_dollars(
    spec: StructureSpec,
    notional_scale: float,
    units: float,
    spot_prev: float,
    rate_prev: float,
    sigma_prev: float,
    years_prev: float,
) -> float:
    if spec.kind == "call_replace":
        d = op.bs_delta("call", spot_prev, spec.strike_primary, rate_prev, sigma_prev, years_prev)
        return notional_scale * d * spot_prev
    if spec.kind == "protective_put":
        dp = op.bs_delta("put", spot_prev, spec.strike_primary, rate_prev, sigma_prev, years_prev)
        return notional_scale * units * (1.0 + dp) * spot_prev
    if spec.kind == "collar":
        assert spec.strike_short_call is not None
        dp = op.bs_delta("put", spot_prev, spec.strike_primary, rate_prev, sigma_prev, years_prev)
        dc = op.bs_delta(
            "call", spot_prev, spec.strike_short_call, rate_prev, sigma_prev, years_prev
        )
        return notional_scale * units * (1.0 + dp - dc) * spot_prev
    raise ValueError(f"unknown structure kind: {spec.kind}")


def simulate_structure(
    inputs: ConvexityInputs,
    spec: StructureSpec,
    sigma_scale: float,
    *,
    notional_scale: float = 1.0,
) -> StructurePath:
    if not (sigma_scale > 0.0 and math.isfinite(sigma_scale)):
        raise ValueError("sigma_scale must be positive and finite")
    if not (notional_scale > 0.0 and math.isfinite(notional_scale)):
        raise ValueError("notional_scale must be positive and finite")

    index = pd.DatetimeIndex(inputs.vxn.index)
    qqq = inputs.qqq_return.to_numpy(dtype=float)
    cash = inputs.cash_return.to_numpy(dtype=float)
    sigma = sigma_scale * inputs.vxn.to_numpy(dtype=float) / 100.0
    rate = inputs.rate.to_numpy(dtype=float)
    bounds = multi_month_bounds(index, spec.step_months)

    day_returns: list[float] = []
    day_exposure: list[float] = []
    day_dates: list[pd.Timestamp] = []
    cycles: list[CycleRecord] = []
    ever_borrowed = False
    bust_date: str | None = None

    for c_start, c_end in bounds:
        positions = np.arange(c_start, c_end + 1)
        spot = np.concatenate([[1.0], np.cumprod(1.0 + qqq[c_start + 1 : c_end + 1])])
        grown = np.concatenate([[1.0], np.cumprod(1.0 + cash[c_start + 1 : c_end + 1])])
        borrow_grown = np.concatenate(
            [[1.0], np.cumprod(1.0 + cash[c_start + 1 : c_end + 1] + MARGIN_SPREAD / TRADING_DAYS)]
        )
        years = (c_end - positions) / TRADING_DAYS
        units = 1.0

        if spec.kind == "call_replace":
            opt_val = np.array(
                [
                    op.bs_price("call", spot[k], spec.strike_primary, rate[p], sigma[p], years[k])
                    for k, p in enumerate(positions)
                ]
            )
            c0 = float(opt_val[0])
            net_prem = notional_scale * c0
            cash_bal = 1.0 - net_prem
            if cash_bal >= 0.0:
                wealth = cash_bal * grown + notional_scale * opt_val
            else:
                ever_borrowed = True
                wealth = notional_scale * opt_val - (-cash_bal) * borrow_grown

        elif spec.kind == "protective_put":
            put_val = np.array(
                [
                    op.bs_price("put", spot[k], spec.strike_primary, rate[p], sigma[p], years[k])
                    for k, p in enumerate(positions)
                ]
            )
            p0 = float(put_val[0])
            units = 1.0 / (1.0 + p0)
            net_prem = p0 * units
            base_wealth = units * (spot + put_val)
            if notional_scale > 1.0 + 1e-12:
                ever_borrowed = True
                wealth = notional_scale * base_wealth - (notional_scale - 1.0) * borrow_grown
            else:
                wealth = base_wealth

        elif spec.kind == "collar":
            assert spec.strike_short_call is not None
            put_val = np.array(
                [
                    op.bs_price("put", spot[k], spec.strike_primary, rate[p], sigma[p], years[k])
                    for k, p in enumerate(positions)
                ]
            )
            call_val = np.array(
                [
                    op.bs_price(
                        "call", spot[k], spec.strike_short_call, rate[p], sigma[p], years[k]
                    )
                    for k, p in enumerate(positions)
                ]
            )
            p0, c0 = float(put_val[0]), float(call_val[0])
            if p0 > c0:
                units = 1.0 / (1.0 + p0 - c0)
                net_prem = (p0 - c0) * units
                base_wealth = units * (spot + put_val - call_val)
            else:
                units = 1.0
                net_prem = p0 - c0
                base_wealth = spot + put_val - call_val + (c0 - p0) * grown
            if notional_scale > 1.0 + 1e-12:
                ever_borrowed = True
                wealth = notional_scale * base_wealth - (notional_scale - 1.0) * borrow_grown
            else:
                wealth = base_wealth

        else:
            raise ValueError(f"unknown structure kind: {spec.kind}")

        for k in range(1, len(positions)):
            if bust_date is not None:
                day_returns.append(0.0)
                day_exposure.append(0.0)
                day_dates.append(index[positions[k]])
                continue
            if wealth[k] <= 0.0:
                bust_date = index[positions[k]].date().isoformat()
                day_returns.append(-1.0)
                day_exposure.append(0.0)
                day_dates.append(index[positions[k]])
                continue
            p_prev = int(positions[k - 1])
            day_returns.append(float(wealth[k] / wealth[k - 1] - 1.0))
            delta_usd = _delta_dollars(
                spec,
                notional_scale,
                units,
                float(spot[k - 1]),
                float(rate[p_prev]),
                float(sigma[p_prev]),
                float(years[k - 1]),
            )
            day_exposure.append(float(delta_usd / wealth[k - 1]))
            day_dates.append(index[positions[k]])

        sq = qqq[c_start + 1 : c_end + 1] ** 2
        rv_pct = 100.0 * math.sqrt(TRADING_DAYS * float(sq.mean()))
        vxn_open = float(inputs.vxn.iloc[c_start])
        cycles.append(
            CycleRecord(
                start=index[c_start].date().isoformat(),
                end=index[c_end].date().isoformat(),
                days=int(c_end - c_start),
                vxn_start=vxn_open,
                net_premium_pct=net_prem,
                realized_vol_pct=rv_pct,
                iv_minus_rv=sigma_scale * vxn_open - rv_pct,
                cycle_return=float(wealth[-1] - 1.0) if bust_date is None else -1.0,
            )
        )

    dates = pd.DatetimeIndex(day_dates)
    return StructurePath(
        returns=pd.Series(day_returns, index=dates),
        exposure=pd.Series(day_exposure, index=dates),
        start=index[bounds[0][0]],
        cycles=tuple(cycles),
        ever_borrowed=ever_borrowed,
        bust_date=bust_date,
    )


def summarize_series(
    returns: pd.Series,
    exposure: pd.Series,
    cash_returns: pd.Series,
    anchor: pd.Timestamp,
) -> dict[str, Any]:
    r = returns.to_numpy(dtype=float)
    c = cash_returns.reindex(returns.index).to_numpy(dtype=float)
    years = (returns.index[-1] - anchor).days / DAYS_PER_YEAR
    eq = np.concatenate([[1.0], np.cumprod(1.0 + r)])
    end_eq = float(eq[-1])
    geo = -1.0 if end_eq <= 0.0 else end_eq ** (1.0 / years) - 1.0
    cash_geo = float(np.prod(1.0 + c)) ** (1.0 / years) - 1.0
    vol = float(np.std(r, ddof=1) * math.sqrt(TRADING_DAYS)) if len(r) > 1 else 0.0
    dd = float(np.min(eq / np.maximum.accumulate(eq) - 1.0))
    q5 = float(np.quantile(r, 0.05))
    tail = r[r <= q5]
    cvar5 = float(np.mean(tail)) if len(tail) > 0 else q5
    excess = r - c
    ex_std = float(np.std(excess, ddof=1)) if len(excess) > 1 else 0.0
    sharpe = float(np.mean(excess) / ex_std * math.sqrt(TRADING_DAYS)) if ex_std > 0.0 else 0.0
    calmar = (geo - cash_geo) / abs(dd) if dd < 0.0 else 0.0
    return {
        "years": years,
        "geometric_annual_return": geo,
        "cash_geo_annual": cash_geo,
        "annual_volatility": vol,
        "max_drawdown": dd,
        "cvar_5pct": cvar5,
        "excess_sharpe": sharpe,
        "excess_calmar": calmar,
        "average_exposure": float(exposure.mean()),
    }


def delta_replica_decomposition(
    inputs: ConvexityInputs, path: StructurePath, opt_summary: Mapping[str, Any]
) -> dict[str, Any]:
    """Compare option path against (1) its daily dynamic Delta spot replica and (2) constant mix."""

    dates = path.returns.index
    qqq = inputs.qqq_return.reindex(dates)
    cash = inputs.cash_return.reindex(dates)
    exp = path.exposure.reindex(dates)

    # Daily-rebalanced spot+cash replica with identical daily Delta exposure.
    replica_r = exp * qqq + (1.0 - exp) * cash
    rep_sum = summarize_series(replica_r, exp, cash, path.start)

    mean_exp = float(exp.mean())
    const_r = mean_exp * qqq + (1.0 - mean_exp) * cash
    const_sum = summarize_series(const_r, pd.Series(mean_exp, index=dates), cash, path.start)

    opt_geo = float(opt_summary["geometric_annual_return"])
    rep_geo = float(rep_sum["geometric_annual_return"])
    const_geo = float(const_sum["geometric_annual_return"])

    return {
        "delta_replica_geo": rep_geo,
        "delta_replica_dd": float(rep_sum["max_drawdown"]),
        "constant_mix_geo": const_geo,
        "constant_mix_dd": float(const_sum["max_drawdown"]),
        "convexity_minus_premium": opt_geo - rep_geo,
        "delta_timing_value": rep_geo - const_geo,
        "total_vs_constant_mix": opt_geo - const_geo,
        "dd_vs_constant_mix": float(opt_summary["max_drawdown"]) - float(const_sum["max_drawdown"]),
    }


def subperiod_stats(
    returns: pd.Series,
    exposure: pd.Series,
    *,
    periods: Sequence[tuple[str, str, date, date]] = SUBPERIODS,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key, label, p_start, p_end in periods:
        mask = (returns.index >= pd.Timestamp(p_start)) & (returns.index <= pd.Timestamp(p_end))
        sub_r = returns.loc[mask]
        sub_e = exposure.loc[mask]
        if sub_r.empty:
            continue
        r = sub_r.to_numpy(dtype=float)
        eq = np.concatenate([[1.0], np.cumprod(1.0 + r)])
        cum = float(eq[-1] - 1.0)
        dd = float(np.min(eq / np.maximum.accumulate(eq) - 1.0))
        out.append(
            {
                "id": key,
                "label": label,
                "days": int(len(sub_r)),
                "cumulative_return": cum,
                "max_drawdown": dd,
                "average_exposure": float(sub_e.mean()),
            }
        )
    return out


def match_drawdown_notional(
    inputs: ConvexityInputs,
    spec: StructureSpec,
    sigma_scale: float,
    target_max_dd: float,
    grid: Sequence[float] = NOTIONAL_GRID,
) -> dict[str, Any]:
    """Find largest notional scale L in grid with max_drawdown >= target_max_dd."""

    chosen_l = grid[0]
    chosen_path = simulate_structure(inputs, spec, sigma_scale, notional_scale=chosen_l)
    chosen_sum = summarize_series(
        chosen_path.returns, chosen_path.exposure, inputs.cash_return, chosen_path.start
    )
    hit_top = False
    for lev in grid[1:]:
        p = simulate_structure(inputs, spec, sigma_scale, notional_scale=lev)
        if p.bust_date is not None:
            break
        s = summarize_series(p.returns, p.exposure, inputs.cash_return, p.start)
        if float(s["max_drawdown"]) >= target_max_dd - 1e-12:
            chosen_l = lev
            chosen_path = p
            chosen_sum = s
            hit_top = lev == grid[-1]
        else:
            break
    return {
        "matched_notional": chosen_l,
        "hit_grid_top": hit_top,
        "ever_borrowed": chosen_path.ever_borrowed,
        "bust_date": chosen_path.bust_date,
        "summary": chosen_sum,
    }


def check_inputs(inputs: ConvexityInputs) -> dict[str, Any]:
    index = pd.DatetimeIndex(inputs.vxn.index)
    bounds3 = multi_month_bounds(index, 3)
    bounds6 = multi_month_bounds(index, 6)
    eval_days = int(bounds3[-1][1] - bounds3[0][0])
    cutoff_ok = inputs.max_loaded_date <= gp.CUTOFF
    ok = cutoff_ok and eval_days >= 2000 and len(bounds3) >= 20 and len(bounds6) >= 10
    return {
        "ok": ok,
        "cutoff_date": gp.CUTOFF.isoformat(),
        "max_loaded_date": inputs.max_loaded_date.isoformat(),
        "cutoff_respected": cutoff_ok,
        "window_start": index[bounds3[0][0]].date().isoformat(),
        "window_end": index[bounds3[-1][1]].date().isoformat(),
        "return_days": eval_days,
        "cycles_3m": len(bounds3),
        "cycles_6m": len(bounds6),
        "filled_days": dict(inputs.filled_days),
        "vxn_mean": float(inputs.vxn.mean()),
        "vxn_median": float(inputs.vxn.median()),
        "rate_mean": float(inputs.rate.mean()),
    }


def evaluate_kill_criteria(
    benchmarks: Mapping[str, Mapping[str, Any]],
    structures: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    hold_geo = float(benchmarks["hold"]["geometric_annual_return"])
    hold_dd = float(benchmarks["hold"]["max_drawdown"])
    f1_geo = float(benchmarks["f0001_1.0"]["geometric_annual_return"])

    details: list[dict[str, Any]] = []
    passed: list[str] = []

    for sid, entry in structures.items():
        s10_base = entry["by_scale"]["1.0"]["base_1x"]
        s10_match = entry["by_scale"]["1.0"]["matched_dd"]
        s11_base = entry["by_scale"]["1.1"]["base_1x"]
        s11_match = entry["by_scale"]["1.1"]["matched_dd"]

        # Choose better of 1.0x or matched-DD at scale 1.0 (both must satisfy DD <= hold).
        cand_mode = "matched_dd"
        if float(s10_base["geometric_annual_return"]) >= float(
            s10_match["summary"]["geometric_annual_return"]
        ):
            cand_mode = "base_1x"

        if cand_mode == "base_1x":
            g10 = float(s10_base["geometric_annual_return"])
            dd10 = float(s10_base["max_drawdown"])
            g11 = float(s11_base["geometric_annual_return"])
            used_l = 1.0
        else:
            g10 = float(s10_match["summary"]["geometric_annual_return"])
            dd10 = float(s10_match["summary"]["max_drawdown"])
            g11 = float(s11_match["summary"]["geometric_annual_return"])
            used_l = float(s10_match["matched_notional"])

        c1_dd = dd10 >= hold_dd - 1e-12
        c2_beat_10 = (g10 - hold_geo >= KILL_MIN_EXCESS_OVER_HOLD) and (g10 > f1_geo)
        c3_pess_11 = (g11 >= hold_geo) and (g11 >= f1_geo - KILL_MAX_PESSIMISTIC_GAP_VS_1X)
        ok = c1_dd and c2_beat_10 and c3_pess_11
        if ok:
            passed.append(sid)

        details.append(
            {
                "structure_id": sid,
                "evaluated_mode": cand_mode,
                "evaluated_notional": used_l,
                "pass_all": ok,
                "checks": {
                    "c1_dd_not_worse_than_hold": c1_dd,
                    "c2_scale_1_0_beats_hold_1pp_and_f0001": c2_beat_10,
                    "c3_pessimistic_1_1_survives": c3_pess_11,
                },
                "deltas": {
                    "scale_1_0_vs_hold": g10 - hold_geo,
                    "scale_1_0_vs_f0001": g10 - f1_geo,
                    "scale_1_1_vs_hold": g11 - hold_geo,
                    "scale_1_1_vs_f0001": g11 - f1_geo,
                },
            }
        )

    verdict = "pass_dev_screen" if passed else "kill_dev_screen"
    text = (
        f"开发窗口初筛通过（{', '.join(passed)}）：值得进入真实 bid/ask 与偏度核实阶段；"
        "非验证窗口结论，非真实可成交收益。"
        if passed
        else "开发窗口止损触发（kill_dev_screen）：即使在 2001–2010 双慢熊与无价差代理下，"
        "所有正凸性结构均未跨过开发止损线，不建议为此购买付费期权数据。"
    )
    return {"verdict": verdict, "passed_structures": passed, "text": text, "details": details}


def run_probe(
    data: BenchData,
    loaded: LoadedSettings,
    root_dir: Path,
    *,
    provenance: Mapping[str, Any],
    specs: Sequence[StructureSpec] | None = None,
) -> dict[str, Any]:
    cut_data = gp.cut(data)
    inputs = ConvexityInputs.from_bench(cut_data)
    chk = check_inputs(inputs)
    if not chk["ok"]:
        return {
            "task_id": TASK_ID,
            "requirement": REQUIREMENT,
            "provenance": dict(provenance),
            "check": chk,
            "stopped": {"reason": "input_check_failed", "text": "输入检查未通过，拒绝运行。"},
            "production_effect": "none",
            "broker_action": "none",
        }

    spec_list = default_structures() if specs is None else tuple(specs)
    # Benchmarks aligned to the exact same 2001-03-01..2010-12-31 dates as option paths.
    ref_path = simulate_structure(inputs, spec_list[0], BASE_SIGMA_SCALE, notional_scale=1.0)
    dates = ref_path.returns.index
    anchor = ref_path.start
    hold_r = inputs.qqq_return.reindex(dates)
    hold_exp = pd.Series(1.0, index=dates)
    hold_sum = summarize_series(hold_r, hold_exp, inputs.cash_return, anchor)

    base_targets = lt.f0001_targets(cut_data, root_dir)
    f1_cfg = avl.StrategyConfig("f0001_1.0", "F0001 原版", "benchmark", "f0001_scaled", 1.0)
    f1_sim = avl.simulate_policy(
        cut_data.returns,
        base_targets,
        cut_data.inputs["vxn"],
        f1_cfg,
        avl.MAIN_COST,
        start=anchor.date(),
        end=dates[-1].date(),
    )
    f1_r = f1_sim.simulation.daily["return"].reindex(dates)
    f1_exp = f1_sim.simulation.daily["exposure"].reindex(dates)
    f1_sum = summarize_series(f1_r, f1_exp, inputs.cash_return, anchor)

    benchmarks = {"hold": hold_sum, "f0001_1.0": f1_sum}
    subperiods_map: dict[str, list[dict[str, Any]]] = {
        "hold": subperiod_stats(hold_r, hold_exp),
        "f0001_1.0": subperiod_stats(f1_r, f1_exp),
    }

    hold_dd = float(hold_sum["max_drawdown"])
    structures_out: dict[str, dict[str, Any]] = {}
    for spec in spec_list:
        by_scale: dict[str, dict[str, Any]] = {}
        for scale in SIGMA_SCALES:
            skey = f"{scale:.1f}"
            p1 = simulate_structure(inputs, spec, scale, notional_scale=1.0)
            s1 = summarize_series(p1.returns, p1.exposure, inputs.cash_return, p1.start)
            decomp = delta_replica_decomposition(inputs, p1, s1)
            matched = match_drawdown_notional(inputs, spec, scale, hold_dd)
            avg_prem = float(np.mean([c.net_premium_pct for c in p1.cycles]))
            avg_iv_rv = float(np.mean([c.iv_minus_rv for c in p1.cycles]))
            by_scale[skey] = {
                "base_1x": s1,
                "decomposition": decomp,
                "matched_dd": matched,
                "cycle_count": len(p1.cycles),
                "avg_cycle_net_premium": avg_prem,
                "avg_iv_minus_rv_vol_pts": avg_iv_rv,
            }
            if math.isclose(scale, BASE_SIGMA_SCALE):
                subperiods_map[spec.structure_id] = subperiod_stats(p1.returns, p1.exposure)
        structures_out[spec.structure_id] = {
            "structure_id": spec.structure_id,
            "label": spec.label,
            "kind": spec.kind,
            "step_months": spec.step_months,
            "by_scale": by_scale,
        }

    kill = evaluate_kill_criteria(benchmarks, structures_out)
    return {
        "task_id": TASK_ID,
        "requirement": REQUIREMENT,
        "provenance": dict(provenance),
        "window": {
            "role": "development_only_pure_qqq",
            "start": chk["window_start"],
            "end": chk["window_end"],
            "cutoff_enforced": gp.CUTOFF.isoformat(),
        },
        "check": chk,
        "benchmarks": benchmarks,
        "structures": structures_out,
        "subperiods_scale_1_0": subperiods_map,
        "verdict": kill,
        "stopped": None,
        "production_effect": "none",
        "broker_action": "none",
    }


def _pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def _pts(x: float) -> str:
    return f"{x * 100:+.2f}"


def _md_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    return lines


def render_markdown(result: Mapping[str, Any]) -> str:
    chk = result["check"]
    bm = result["benchmarks"]
    hold_geo = bm["hold"]["geometric_annual_return"]
    f1_geo = bm["f0001_1.0"]["geometric_annual_return"]

    lines = [
        "# TRADING-2574：纯 QQQ 期权正凸性结构（VXN + Black-Scholes 代理）开发窗口诊断",
        "",
        f"- 任务：`{result['task_id']}`；需求：`{result['requirement']}`",
        f"- 评估窗口：纯 QQQ 开发窗口 `{chk['window_start']}..{chk['window_end']}`"
        f"（共 `{chk['return_days']}` 个交易日，机械截断于 `{chk['cutoff_date']}`，"
        "零 2011+ 数据读取）",
        f"- 周期与输入：`3M` 周期 `{chk['cycles_3m']}` 个，`6M` 周期 `{chk['cycles_6m']}` 个；"
        f"`VXN` 均值 `{chk['vxn_mean']:.1f}`（填补 `{chk['filled_days']['vxn']}` 天），"
        f"无风险利率均值 `{_pct(chk['rate_mean'])}`",
        f"- 判定：**{result['verdict']['verdict']}** —— {result['verdict']['text']}",
        "- 代理局限：无分红 Black-Scholes、`sigma_scale * VXN`、无真实 `bid/ask` 与偏度（Skew）；"
        "`sigma_scale = 1.1` 模拟买方溢价/价差惩罚，非真实成交。",
        "",
        "## 1. 主表（`sigma_scale = 1.0`）：1.0x 名义与同回撤配平 $L^*$ 表现",
        "",
    ]

    rows = [
        [
            "`hold`",
            "买入持有 QQQ (基准)",
            _pct(bm["hold"]["geometric_annual_return"]),
            "+0.00",
            _pct(bm["hold"]["max_drawdown"]),
            _pct(bm["hold"]["annual_volatility"]),
            f"{bm['hold']['excess_sharpe']:.2f}",
            f"{bm['hold']['average_exposure']:.2f}",
            "—",
            "—",
            "—",
        ],
        [
            "`f0001_1.0`",
            "F0001 原版 (现货趋势标杆)",
            _pct(bm["f0001_1.0"]["geometric_annual_return"]),
            _pts(f1_geo - hold_geo),
            _pct(bm["f0001_1.0"]["max_drawdown"]),
            _pct(bm["f0001_1.0"]["annual_volatility"]),
            f"{bm['f0001_1.0']['excess_sharpe']:.2f}",
            f"{bm['f0001_1.0']['average_exposure']:.2f}",
            "—",
            "—",
            "—",
        ],
    ]
    for sid, entry in result["structures"].items():
        s1 = entry["by_scale"]["1.0"]["base_1x"]
        m = entry["by_scale"]["1.0"]["matched_dd"]
        ms = m["summary"]
        borrow_tag = "需融资" if m["ever_borrowed"] else "全自融资"
        rows.append(
            [
                f"`{sid}`",
                entry["label"],
                _pct(s1["geometric_annual_return"]),
                _pts(s1["geometric_annual_return"] - hold_geo),
                _pct(s1["max_drawdown"]),
                _pct(s1["annual_volatility"]),
                f"{s1['excess_sharpe']:.2f}",
                f"{s1['average_exposure']:.2f}",
                f"{m['matched_notional']:.2f}x ({borrow_tag})",
                _pct(ms["geometric_annual_return"]),
                _pct(ms["max_drawdown"]),
            ]
        )
    lines += _md_table(
        [
            "结构",
            "说明",
            "1x 年化",
            "1x 对 Hold",
            "1x 最大回撤",
            "1x 波动",
            "超额夏普",
            "平均Delta",
            "同回撤 L*",
            "L* 年化",
            "L* 回撤",
        ],
        rows,
    )

    lines += ["## 2. Delta 副本拆账与波动率保费（`sigma_scale = 1.0`，1.0x 名义）", ""]
    d_rows = []
    for sid, entry in result["structures"].items():
        b10 = entry["by_scale"]["1.0"]
        s1 = b10["base_1x"]
        dc = b10["decomposition"]
        d_rows.append(
            [
                f"`{sid}`",
                _pct(s1["geometric_annual_return"]),
                _pct(dc["delta_replica_geo"]),
                _pct(dc["constant_mix_geo"]),
                _pts(dc["convexity_minus_premium"]),
                _pts(dc["delta_timing_value"]),
                _pts(dc["dd_vs_constant_mix"]),
                _pct(b10["avg_cycle_net_premium"]),
                f"{b10['avg_iv_minus_rv_vol_pts']:+.1f} pts",
            ]
        )
    lines += _md_table(
        [
            "结构",
            "期权净年化",
            "动态Delta副本",
            "同敞口固定混合",
            "正凸性减保费",
            "自动调Delta价值",
            "回撤较固定改善",
            "单周期净权利金",
            "平均 IV−RV",
        ],
        d_rows,
    )

    lines += ["## 3. 定价敏感度（`sigma_scale ∈ {0.9, 1.0, 1.1}`：买方悲观成本压力）", ""]
    s_rows = []
    for sid, entry in result["structures"].items():
        parts = []
        for skey in ("0.9", "1.0", "1.1"):
            b = entry["by_scale"][skey]
            g1 = b["base_1x"]["geometric_annual_return"]
            gm = b["matched_dd"]["summary"]["geometric_annual_return"]
            lv = b["matched_dd"]["matched_notional"]
            parts.append(f"1x {_pct(g1)} / L*({lv:.2f}x) {_pct(gm)}")
        s_rows.append([f"`{sid}`", *parts])
    lines += _md_table(
        ["结构", "乐观 0.9×VXN", "基准 1.0×VXN", "买方悲观 1.1×VXN"],
        s_rows,
    )

    lines += ["## 4. 危机与长牛分段表现（`sigma_scale = 1.0`，1.0x 名义）", ""]
    sub_map = result["subperiods_scale_1_0"]
    sub_headers = ["结构", *[sp["label"] for sp in sub_map["hold"]]]
    sub_rows = []
    for sid in ("hold", "f0001_1.0", *result["structures"].keys()):
        parts = [
            f"{_pct(sp['cumulative_return'])} / DD {_pct(sp['max_drawdown'])}"
            for sp in sub_map[sid]
        ]
        sub_rows.append([f"`{sid}`", *parts])
    lines += _md_table(sub_headers, sub_rows)

    lines += ["## 5. 开发窗口止损线逐项核对", ""]
    k_rows = []
    for item in result["verdict"]["details"]:
        c = item["checks"]
        d = item["deltas"]
        k_rows.append(
            [
                f"`{item['structure_id']}` ({item['evaluated_notional']:.2f}x)",
                "PASS" if c["c1_dd_not_worse_than_hold"] else "FAIL",
                f"{'PASS' if c['c2_scale_1_0_beats_hold_1pp_and_f0001'] else 'FAIL'} "
                f"(对Hold{_pts(d['scale_1_0_vs_hold'])}/对1x{_pts(d['scale_1_0_vs_f0001'])})",
                f"{'PASS' if c['c3_pessimistic_1_1_survives'] else 'FAIL'} "
                f"(对Hold{_pts(d['scale_1_1_vs_hold'])}/对1x{_pts(d['scale_1_1_vs_f0001'])})",
                "**PASS**" if item["pass_all"] else "**KILL**",
            ]
        )
    lines += _md_table(
        [
            "结构（取优口径）",
            "1.回撤≤Hold",
            "2.基准1.0胜Hold+1且胜F0001",
            "3.悲观1.1不输Hold且距F0001≤0.5",
            "总判",
        ],
        k_rows,
    )

    lines += [
        "---",
        "说明：本表仅为 `2001-02-28..2010-12-31` 开发窗口的 VXN+BS 理论代理诊断，"
        "不构成验证窗口结论、真实期权成交结果或投资建议。",
        "`production_effect=none`；`broker_action=none`。",
        "",
    ]
    return "\n".join(lines)


def load_qqq_dev_data(snapshot_root: Path) -> tuple[BenchData, LoadedSettings, Path]:
    loaded = load_settings()
    root_dir = lg.research_root(loaded)
    _, plan = tr.load_trial(root_dir, DATA_TRIAL)
    data = from_snapshot(
        snapshot_root / plan.data.snapshot_id,
        qqq_series="actual",
    )
    return gp.cut(data), loaded, root_dir


def write_check(check: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir.joinpath(CHECK_JSON).write_text(
        json.dumps(gp._clean(dict(check)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir.joinpath(RESULT_JSON).write_text(
        json.dumps(gp._clean(dict(result)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    out_dir.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """TRADING-2574：纯 QQQ 期权正凸性结构（VXN+BS 代理）开发窗口诊断。"""


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option(help="输入检查结果保存目录。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    try:
        data, _, _ = load_qqq_dev_data(root)
        inputs = ConvexityInputs.from_bench(data)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"option_convexity_proxy: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None
    chk = check_inputs(inputs)
    write_check(chk, out)
    typer.echo(
        f"option_convexity_proxy: QQQ {chk['return_days']} 天，"
        f"3M/6M 周期 {chk['cycles_3m']}/{chk['cycles_6m']}，"
        f"截断上限 {chk['max_loaded_date']}；输入检查 {'PASS' if chk['ok'] else 'FAIL'}"
    )
    if not chk["ok"]:
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果输出目录（已有结果拒绝覆盖）。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("option_convexity_proxy: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, loaded, root_dir = load_qqq_dev_data(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"option_convexity_proxy: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None
    result = run_probe(data, loaded, root_dir, provenance=git_provenance(PROJECT_ROOT))
    write_check(result["check"], out)
    if result["stopped"] is not None:
        typer.echo(result["stopped"]["text"])
        raise typer.Exit(code=1)
    write_result(result, out)
    typer.echo(f"option_convexity_proxy: 判定 {result['verdict']['verdict']}；结果目录 {out}")


if __name__ == "__main__":
    app()
