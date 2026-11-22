"""Pure-QQQ asymmetric VXN-gated low-leverage development probe (TRADING-2573).

Owner instruction (2026-11-22): test whether decoupling a monthly trend base (F0001)
from a faster weekly VXN volatility-gated low-leverage sleeve (L_max in {1.3, 1.5},
Scheme A margin/futures financing) improves QQQ compound return without deepening
maximum drawdown relative to buy-and-hold QQQ.

Scope and discipline:
- Pure QQQ only (no US market proxy), mechanically cut at 2010-12-31 before any
  signal or metric computation (zero 2011+ data read).
- Window: 2001-02-28 close to 2010-12-31 (first month-end after VXNCLS begins on
  2001-02-02, so both F0001 and VXN are initialized cleanly).
- Print-only development diagnostic: does not enter trial ledgers, alter gates or
  rulers, touch daily reports, or authorize production/broker actions.
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
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR, summarize
from ai_trading_system.research_bench.settings import LoadedSettings, load_settings
from ai_trading_system.research_bench.simulate import Simulation, schedule_mask
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import leverage_table as lt

TASK_ID = "TRADING-2573_QQQ_ASYM_VXN_LEVERAGE"
REQUIREMENT = "docs/requirements/TRADING-2573_QQQ_Asym_VXN_Leverage_Dev_Probe.md"
DATA_TRIAL = lt.DATA_TRIAL
TRADING_DAYS = 252

# All parameters below are the temporary development-diagnostic baseline documented in
# docs/requirements/TRADING-2573_QQQ_Asym_VXN_Leverage_Dev_Probe.md; exit upon owner
# review of the development diagnostic verdict (not production or promotion thresholds).
VXN_FIRST_DATE = date(2001, 2, 2)
DEV_START = date(2001, 2, 28)
DEV_END = date(2010, 12, 31)
VXN_ON_MAX = 20.0
VXN_OFF_MIN = 25.0
INVVOL_TARGET_VXN = 25.0
INVVOL_DEADBAND = 0.10
LEVERAGE_CAPS: tuple[float, ...] = (1.3, 1.5)
KILL_MIN_EXCESS_OVER_HOLD = 0.010
KILL_DD_IMPROVEMENT_OVER_UNFILTERED = 0.010
KILL_MAX_RETURN_GIVEUP_FOR_DD = 0.005

# Named sub-periods inside the 2001-2010 QQQ development window for crisis diagnostics.
SUBPERIODS: tuple[tuple[str, str, date, date], ...] = (
    ("dotcom_tail", "2001-03..2002-10 泡沫尾段", date(2001, 3, 1), date(2002, 10, 31)),
    ("long_bull", "2002-11..2007-10 高波慢牛", date(2002, 11, 1), date(2007, 10, 31)),
    ("gfc_bear", "2007-11..2009-03 金融危机", date(2007, 11, 1), date(2009, 3, 31)),
    ("rebound", "2009-04..2010-12 震荡反弹", date(2009, 4, 1), date(2010, 12, 31)),
)

CHECK_JSON = "asym_vxn_leverage_check.json"
RESULT_JSON = "asym_vxn_leverage.json"
RESULT_MD = "asym_vxn_leverage.md"

PolicyMode = Literal["hold", "f0001_scaled", "asym_band", "monthly_band", "asym_invvol"]


class CostModel(NamedTuple):
    name: str
    label: str
    spread: float
    bps: float
    fixed_borrow_rate: float | None = None


MAIN_COST = CostModel("main", "主算（DTB3+1%，4 bps）", spread=0.01, bps=4.0)
ROBUST_COST = CostModel("robust", "稳健（DTB3+2%，25 bps）", spread=0.02, bps=25.0)
HIGH_RATE_5PCT = CostModel(
    "high_rate_5pct",
    "高息压力（借钱固定 5%/年，4 bps）",
    spread=0.0,
    bps=4.0,
    fixed_borrow_rate=0.05,
)
COST_MODELS: tuple[CostModel, ...] = (MAIN_COST, ROBUST_COST, HIGH_RATE_5PCT)


@dataclass(frozen=True)
class StrategyConfig:
    strategy_id: str
    label: str
    group: str
    mode: PolicyMode
    leverage_cap: float
    vxn_on: float = VXN_ON_MAX
    vxn_off: float = VXN_OFF_MIN
    invvol_target: float = INVVOL_TARGET_VXN
    deadband: float = INVVOL_DEADBAND


def default_configs() -> tuple[StrategyConfig, ...]:
    items: list[StrategyConfig] = [
        StrategyConfig("hold", "买入持有 QQQ (1.0x)", "benchmark", "hold", 1.0),
        StrategyConfig("f0001_1.0", "F0001 原版 (1.0x 月频)", "benchmark", "f0001_scaled", 1.0),
    ]
    for cap in LEVERAGE_CAPS:
        tag = f"{cap:.1f}"
        items.append(
            StrategyConfig(
                f"f0001_{tag}",
                f"F0001 无波动门直接放大 ({tag}x 月频)",
                "unfiltered_control",
                "f0001_scaled",
                cap,
            )
        )
    for cap in LEVERAGE_CAPS:
        tag = f"{cap:.1f}"
        items.append(
            StrategyConfig(
                f"asym_band_{tag}",
                f"双门滞回不对称 ({tag}x, VXN≤20开/≥25退, 周频去杠杆)",
                "primary",
                "asym_band",
                cap,
            )
        )
    for cap in LEVERAGE_CAPS:
        tag = f"{cap:.1f}"
        items.append(
            StrategyConfig(
                f"asym_invvol_{tag}",
                f"双门反比平滑 ({tag}x, clip(25/VXN), 周频去杠杆)",
                "smooth_control",
                "asym_invvol",
                cap,
            )
        )
    for cap in LEVERAGE_CAPS:
        tag = f"{cap:.1f}"
        items.append(
            StrategyConfig(
                f"monthly_band_{tag}",
                f"双门滞回月频对照 ({tag}x, 杠杆也月频)",
                "frequency_control",
                "monthly_band",
                cap,
            )
        )
    return tuple(items)


class SimRun(NamedTuple):
    simulation: Simulation
    bust: pd.Timestamp | None
    weekly_delever_triggers: int
    weekly_relever_triggers: int
    vxn_missing_decisions: int


app = typer.Typer(help="TRADING-2573：纯 QQQ 非对称 VXN 波动门低倍择时杠杆开发窗口诊断。")


def _finite_nonneg(value: float, label: str) -> float:
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"{label} must be finite and nonnegative, got {value}")
    return value


def _next_hysteresis(current: bool, vxn_value: float, on_max: float, off_min: float) -> bool:
    if not math.isfinite(vxn_value):
        return False
    if vxn_value <= on_max:
        return True
    if vxn_value >= off_min:
        return False
    return current


def _invvol_target(vxn_value: float, cap: float, target_vxn: float) -> float:
    if not math.isfinite(vxn_value) or vxn_value <= 0.0:
        return 1.0
    return float(min(cap, max(1.0, target_vxn / vxn_value)))


def simulate_policy(
    returns: pd.DataFrame,
    base_targets: pd.Series,
    vxn: pd.Series,
    config: StrategyConfig,
    cost: CostModel,
    *,
    start: date = DEV_START,
    end: date = DEV_END,
) -> SimRun:
    """Simulate Scheme A margin/futures financing with exact t-close -> t+1-close lag."""

    index = pd.DatetimeIndex(returns.index)
    if not base_targets.index.equals(index) or not vxn.index.equals(index):
        raise ValueError("returns, base_targets and vxn must share the exact same DatetimeIndex")
    if end > gp.CUTOFF:
        raise ValueError(f"development probe refuses end date after {gp.CUTOFF}: {end}")

    first = int(index.searchsorted(pd.Timestamp(start)))
    last = int(index.searchsorted(pd.Timestamp(end), side="right")) - 1
    if first >= len(index) or last <= first:
        raise ValueError(f"no return dates between {start} and {end}")

    matrix = returns[["qqq", "cash"]].to_numpy(dtype=float)
    if not np.isfinite(matrix[first + 1 : last + 1]).all():
        raise ValueError("qqq or cash has missing or nonfinite returns inside the window")

    base_vals = base_targets.to_numpy(dtype=float)
    vxn_vals = vxn.to_numpy(dtype=float)
    month_end = schedule_mask(index, "month_end")
    week_end = schedule_mask(index, "week_end")

    opening = next((p for p in range(first - 1, -1, -1) if month_end[p]), None)
    if opening is None:
        raise ValueError("no executable month-end base target before start; extend lookback")

    if config.mode == "hold":
        base_w = 1.0
        lev_on = False
        active_target = 1.0
    else:
        base_w = _finite_nonneg(float(base_vals[opening]), "opening base target")
        if config.mode == "f0001_scaled":
            lev_on = False
            active_target = config.leverage_cap * base_w
        elif config.mode in ("asym_band", "monthly_band"):
            lev_on = _next_hysteresis(
                False, float(vxn_vals[opening]), config.vxn_on, config.vxn_off
            )
            active_target = config.leverage_cap if (base_w >= 1.0 - 1e-12 and lev_on) else base_w
        elif config.mode == "asym_invvol":
            lev_on = False
            desired = _invvol_target(
                float(vxn_vals[opening]), config.leverage_cap, config.invvol_target
            )
            active_target = desired if base_w >= 1.0 - 1e-12 else base_w
        else:
            raise ValueError(f"unknown policy mode: {config.mode}")

    weight = active_target
    rows: list[tuple[float, float, float, float, float, float, float]] = []
    bust: pd.Timestamp | None = None
    weekly_delever = 0
    weekly_relever = 0
    vxn_missing = 0

    for q in range(first + 1, last + 1):
        if bust is not None:
            rows.append((0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
            continue

        market, cash = float(matrix[q, 0]), float(matrix[q, 1])
        held = weight
        borrowed = max(held - 1.0, 0.0)
        if borrowed > 0.0:
            if cost.fixed_borrow_rate is not None:
                financing_cost = borrowed * (cost.fixed_borrow_rate / TRADING_DAYS)
            else:
                financing_cost = borrowed * (cash + cost.spread / TRADING_DAYS)
            gross = held * market - financing_cost
        else:
            financing_cost = 0.0
            gross = held * market + (1.0 - held) * cash

        if 1.0 + gross <= 0.0:
            bust = index[q]
            rows.append((-1.0, gross, held, borrowed, 0.0, 0.0, financing_cost))
            continue

        # Drifted weight at close of day q before any scheduled trade at close q.
        weight = held * (1.0 + market) / (1.0 + gross)

        decided = q - 1
        is_month = month_end[decided]
        is_week = week_end[decided]
        turnover = 0.0

        if config.mode == "hold":
            pass
        elif config.mode == "f0001_scaled":
            if is_month:
                base_w = _finite_nonneg(float(base_vals[decided]), "base target")
                active_target = config.leverage_cap * base_w
                turnover = abs(active_target - weight)
                weight = active_target
        elif config.mode in ("asym_band", "monthly_band"):
            v = float(vxn_vals[decided])
            if is_month:
                if not math.isfinite(v):
                    vxn_missing += 1
                base_w = _finite_nonneg(float(base_vals[decided]), "base target")
                lev_on = _next_hysteresis(lev_on, v, config.vxn_on, config.vxn_off)
                active_target = (
                    config.leverage_cap if (base_w >= 1.0 - 1e-12 and lev_on) else base_w
                )
                turnover = abs(active_target - weight)
                weight = active_target
            elif config.mode == "asym_band" and is_week:
                if not math.isfinite(v):
                    vxn_missing += 1
                new_lev_on = _next_hysteresis(lev_on, v, config.vxn_on, config.vxn_off)
                if base_w >= 1.0 - 1e-12 and new_lev_on != lev_on:
                    if lev_on and not new_lev_on:
                        weekly_delever += 1
                    elif not lev_on and new_lev_on:
                        weekly_relever += 1
                    lev_on = new_lev_on
                    active_target = config.leverage_cap if lev_on else 1.0
                    turnover = abs(active_target - weight)
                    weight = active_target
                else:
                    lev_on = new_lev_on
        elif config.mode == "asym_invvol":
            v = float(vxn_vals[decided])
            if is_month:
                if not math.isfinite(v):
                    vxn_missing += 1
                base_w = _finite_nonneg(float(base_vals[decided]), "base target")
                desired = _invvol_target(v, config.leverage_cap, config.invvol_target)
                active_target = desired if base_w >= 1.0 - 1e-12 else base_w
                turnover = abs(active_target - weight)
                weight = active_target
            elif is_week:
                if not math.isfinite(v):
                    vxn_missing += 1
                if base_w >= 1.0 - 1e-12:
                    desired = _invvol_target(v, config.leverage_cap, config.invvol_target)
                    force_off = desired <= 1.0 + 1e-12 and active_target > 1.0 + 1e-12
                    if force_off or abs(desired - active_target) >= config.deadband - 1e-12:
                        if force_off:
                            weekly_delever += 1
                        elif active_target <= 1.0 + 1e-12 and desired > 1.0 + 1e-12:
                            weekly_relever += 1
                        active_target = desired
                        turnover = abs(active_target - weight)
                        weight = active_target

        trade_cost = turnover * cost.bps / 10_000.0
        net = (1.0 + gross) * (1.0 - trade_cost) - 1.0
        rows.append((net, gross, held, borrowed, turnover, trade_cost, financing_cost))

    daily = pd.DataFrame(
        rows,
        index=index[first + 1 : last + 1],
        columns=[
            "return",
            "gross_return",
            "exposure",
            "borrowed",
            "turnover",
            "cost",
            "financing_cost",
        ],
    )
    return SimRun(
        simulation=Simulation(daily=daily, start=index[first]),
        bust=bust,
        weekly_delever_triggers=weekly_delever,
        weekly_relever_triggers=weekly_relever,
        vxn_missing_decisions=vxn_missing,
    )


def summarize_run(
    sim_run: SimRun, cash_returns: pd.Series, loaded: LoadedSettings
) -> dict[str, Any]:
    daily = sim_run.simulation.daily
    base = summarize(sim_run.simulation, cash_returns, loaded)
    r_net = daily["return"].to_numpy(dtype=float)
    r_cash = cash_returns.reindex(daily.index).to_numpy(dtype=float)
    years = (daily.index[-1] - sim_run.simulation.start).days / DAYS_PER_YEAR
    cash_equity = float(np.prod(1.0 + r_cash))
    cash_geo = cash_equity ** (1.0 / years) - 1.0

    excess = r_net - r_cash
    excess_std = float(np.std(excess, ddof=1)) if len(excess) > 1 else 0.0
    excess_sharpe = (
        float(np.mean(excess)) / excess_std * math.sqrt(TRADING_DAYS) if excess_std > 0.0 else 0.0
    )
    max_dd = float(base["max_drawdown"])
    geo = float(base["geometric_annual_return"])
    vol = float(base["annual_volatility"])
    calmar = (geo - cash_geo) / abs(max_dd) if max_dd < 0.0 else 0.0

    arith_annual = float(np.sum(r_net)) / years
    log_annual = float(np.sum(np.log1p(np.maximum(r_net, -0.999999)))) / years
    vol_drag_quad = 0.5 * vol * vol
    vol_drag_realized = arith_annual - log_annual
    financing_drag = float(daily["financing_cost"].sum()) / years
    turnover_drag = float(daily["cost"].sum()) / years

    exposure = daily["exposure"].to_numpy(dtype=float)
    borrowed = daily["borrowed"].to_numpy(dtype=float)
    return {
        **base,
        "years": years,
        "cash_geo_annual": cash_geo,
        "excess_sharpe": excess_sharpe,
        "excess_calmar": calmar,
        "arith_annual_return": arith_annual,
        "log_annual_return": log_annual,
        "vol_drag_quadratic": vol_drag_quad,
        "vol_drag_realized": vol_drag_realized,
        "financing_drag_annual": financing_drag,
        "turnover_drag_annual": turnover_drag,
        "average_borrowed": float(np.mean(borrowed)),
        "levered_day_share": float(np.mean(exposure > 1.0 + 1e-6)),
        "max_exposure": float(np.max(exposure)),
        "weekly_delever_triggers": sim_run.weekly_delever_triggers,
        "weekly_relever_triggers": sim_run.weekly_relever_triggers,
        "vxn_missing_decisions": sim_run.vxn_missing_decisions,
        "bust_date": None if sim_run.bust is None else sim_run.bust.date().isoformat(),
    }


def subperiod_diagnostics(
    sim_run: SimRun,
    *,
    periods: Sequence[tuple[str, str, date, date]] = SUBPERIODS,
) -> list[dict[str, Any]]:
    daily = sim_run.simulation.daily
    out: list[dict[str, Any]] = []
    for key, label, p_start, p_end in periods:
        sub = daily.loc[
            (daily.index >= pd.Timestamp(p_start)) & (daily.index <= pd.Timestamp(p_end))
        ]
        if sub.empty:
            continue
        r = sub["return"].to_numpy(dtype=float)
        eq = np.concatenate([[1.0], np.cumprod(1.0 + r)])
        cum = float(eq[-1] - 1.0)
        dd = float(np.min(eq / np.maximum.accumulate(eq) - 1.0))
        first_exp = float(sub["exposure"].iloc[0])
        out.append(
            {
                "id": key,
                "label": label,
                "days": int(len(sub)),
                "cumulative_return": cum,
                "max_drawdown": dd,
                "average_exposure": float(sub["exposure"].mean()),
                "levered_day_share": float((sub["exposure"] > 1.0 + 1e-6).mean()),
                "entry_exposure": first_exp,
                "entry_delevered": bool(first_exp <= 1.0 + 1e-6),
            }
        )
    return out


def check_inputs(
    data: BenchData,
    *,
    start: date = DEV_START,
    end: date = DEV_END,
) -> dict[str, Any]:
    index = pd.DatetimeIndex(data.returns.index)
    max_date = index.max().date()
    in_window = (index > pd.Timestamp(start)) & (index <= pd.Timestamp(end))
    vxn_window = (index >= pd.Timestamp(VXN_FIRST_DATE)) & (index <= pd.Timestamp(end))
    ret_sub = data.returns.loc[in_window, ["qqq", "cash"]]
    vxn_sub = data.inputs.loc[vxn_window, "vxn"]
    missing_returns = int((~np.isfinite(ret_sub.to_numpy(dtype=float))).sum())
    missing_vxn = int(vxn_sub.isna().sum())
    vxn_valid = vxn_sub.dropna()
    vxn_le_20 = float((vxn_valid <= VXN_ON_MAX).mean()) if not vxn_valid.empty else 0.0
    vxn_ge_25 = float((vxn_valid >= VXN_OFF_MIN).mean()) if not vxn_valid.empty else 0.0
    cutoff_ok = max_date <= gp.CUTOFF
    ok = cutoff_ok and missing_returns == 0 and len(ret_sub) >= 2000 and not vxn_valid.empty
    return {
        "ok": ok,
        "cutoff_date": gp.CUTOFF.isoformat(),
        "max_loaded_date": max_date.isoformat(),
        "cutoff_respected": cutoff_ok,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "return_days": int(len(ret_sub)),
        "missing_return_cells": missing_returns,
        "vxn_span_days": int(len(vxn_sub)),
        "vxn_missing_days": missing_vxn,
        "vxn_first_valid": None if vxn_valid.empty else vxn_valid.index[0].date().isoformat(),
        "vxn_share_le_20": vxn_le_20,
        "vxn_share_ge_25": vxn_ge_25,
        "snapshot_source": dict(data.source),
    }


def evaluate_kill_criteria(
    summaries: Mapping[str, Mapping[str, Mapping[str, Any]]],
    configs: Sequence[StrategyConfig],
) -> dict[str, Any]:
    """Evaluate preregistered development-window kill criteria on primary configs."""

    hold_main = summaries["main"]["hold"]
    hold_robust = summaries["robust"]["hold"]
    base_main = summaries["main"]["f0001_1.0"]
    base_high = summaries["high_rate_5pct"]["f0001_1.0"]

    details: list[dict[str, Any]] = []
    passed_ids: list[str] = []
    for cfg in configs:
        if cfg.group != "primary":
            continue
        sid = cfg.strategy_id
        ctrl_id = f"f0001_{cfg.leverage_cap:.1f}"
        m = summaries["main"][sid]
        r = summaries["robust"][sid]
        h = summaries["high_rate_5pct"][sid]
        ctrl = summaries["main"][ctrl_id]

        c1_dd = float(m["max_drawdown"]) >= float(hold_main["max_drawdown"]) - 1e-12
        c2_beat_hold = float(m["geometric_annual_return"]) - float(
            hold_main["geometric_annual_return"]
        ) >= KILL_MIN_EXCESS_OVER_HOLD and float(r["geometric_annual_return"]) >= float(
            hold_robust["geometric_annual_return"]
        )
        c3_beat_1x = float(m["geometric_annual_return"]) > float(
            base_main["geometric_annual_return"]
        ) and float(h["geometric_annual_return"]) > float(base_high["geometric_annual_return"])
        ret_vs_ctrl = float(m["geometric_annual_return"]) - float(ctrl["geometric_annual_return"])
        dd_vs_ctrl = float(m["max_drawdown"]) - float(ctrl["max_drawdown"])
        c4_beat_unfiltered = ret_vs_ctrl > 0.0 or (
            dd_vs_ctrl >= KILL_DD_IMPROVEMENT_OVER_UNFILTERED
            and ret_vs_ctrl >= -KILL_MAX_RETURN_GIVEUP_FOR_DD
        )
        all_ok = c1_dd and c2_beat_hold and c3_beat_1x and c4_beat_unfiltered
        if all_ok:
            passed_ids.append(sid)
        details.append(
            {
                "strategy_id": sid,
                "unfiltered_control": ctrl_id,
                "pass_all": all_ok,
                "checks": {
                    "c1_dd_not_worse_than_hold": c1_dd,
                    "c2_beat_hold_main_1pp_and_robust_ge_0": c2_beat_hold,
                    "c3_beat_f0001_1x_main_and_5pct_rate": c3_beat_1x,
                    "c4_value_add_over_unfiltered": c4_beat_unfiltered,
                },
                "deltas": {
                    "main_vs_hold": float(m["geometric_annual_return"])
                    - float(hold_main["geometric_annual_return"]),
                    "robust_vs_hold": float(r["geometric_annual_return"])
                    - float(hold_robust["geometric_annual_return"]),
                    "main_vs_f0001_1x": float(m["geometric_annual_return"])
                    - float(base_main["geometric_annual_return"]),
                    "high_rate_5pct_vs_f0001_1x": float(h["geometric_annual_return"])
                    - float(base_high["geometric_annual_return"]),
                    "main_vs_unfiltered": ret_vs_ctrl,
                    "dd_vs_unfiltered": dd_vs_ctrl,
                },
            }
        )

    verdict = "pass_dev_screen" if passed_ids else "kill_dev_screen"
    text = (
        f"开发窗口初筛通过（{', '.join(passed_ids)}），可讨论后续预注册设计；非验证窗口结论。"
        if passed_ids
        else (
            "开发窗口止损触发（kill_dev_screen）："
            "主测配置未全部跨过四道开发止损线，不调参、不读验证窗口。"
        )
    )
    return {"verdict": verdict, "passed_primary_ids": passed_ids, "text": text, "details": details}


def run_probe(
    data: BenchData,
    loaded: LoadedSettings,
    root_dir: Path,
    *,
    provenance: Mapping[str, Any],
    configs: Sequence[StrategyConfig] | None = None,
    start: date = DEV_START,
    end: date = DEV_END,
) -> dict[str, Any]:
    cut_data = gp.cut(data)
    check = check_inputs(cut_data, start=start, end=end)
    if not check["ok"]:
        return {
            "task_id": TASK_ID,
            "requirement": REQUIREMENT,
            "provenance": dict(provenance),
            "check": check,
            "stopped": {"reason": "input_check_failed", "text": "输入检查未通过，拒绝运行。"},
            "production_effect": "none",
            "broker_action": "none",
        }

    cfg_list = default_configs() if configs is None else tuple(configs)
    base_targets = lt.f0001_targets(cut_data, root_dir)
    vxn = cut_data.inputs["vxn"]
    cash_returns = cut_data.returns["cash"]

    summaries: dict[str, dict[str, dict[str, Any]]] = {}
    subperiods: dict[str, list[dict[str, Any]]] = {}

    for cost in COST_MODELS:
        summaries[cost.name] = {}
        for cfg in cfg_list:
            sim_run = simulate_policy(
                cut_data.returns, base_targets, vxn, cfg, cost, start=start, end=end
            )
            summaries[cost.name][cfg.strategy_id] = summarize_run(sim_run, cash_returns, loaded)
            if cost.name == MAIN_COST.name:
                subperiods[cfg.strategy_id] = subperiod_diagnostics(sim_run)

    kill = evaluate_kill_criteria(summaries, cfg_list)
    return {
        "task_id": TASK_ID,
        "requirement": REQUIREMENT,
        "provenance": dict(provenance),
        "window": {
            "role": "development_only_pure_qqq",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "cutoff_enforced": gp.CUTOFF.isoformat(),
        },
        "check": check,
        "configs": [
            {
                "strategy_id": c.strategy_id,
                "label": c.label,
                "group": c.group,
                "mode": c.mode,
                "leverage_cap": c.leverage_cap,
            }
            for c in cfg_list
        ],
        "summaries": summaries,
        "subperiods_main": subperiods,
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
    lines = [
        "# TRADING-2573：纯 QQQ 非对称 VXN 波动门低倍择时杠杆开发窗口诊断",
        "",
        f"- 任务：`{result['task_id']}`；需求：`{result['requirement']}`",
        f"- 评估窗口：纯 QQQ 开发窗口 `{chk['window_start']}..{chk['window_end']}`"
        f"（共 `{chk['return_days']}` 个交易日，机械截断于 `{chk['cutoff_date']}`，"
        "零 2011+ 数据读取）",
        f"- 数据质量检查：`{'PASS' if chk['ok'] else 'FAIL'}`；"
        f"`VXN` 缺失 `{chk['vxn_missing_days']}` 天；"
        f"`VXN ≤ 20` 占比 `{_pct(chk['vxn_share_le_20'])}`，"
        f"`VXN ≥ 25` 占比 `{_pct(chk['vxn_share_ge_25'])}`",
        f"- 判定：**{result['verdict']['verdict']}** —— {result['verdict']['text']}",
        "",
        "## 1. 主表：收益、回撤与单位风险效率（主算 / 稳健 / 5% 高息压力）",
        "",
    ]

    main_s = result["summaries"]["main"]
    rob_s = result["summaries"]["robust"]
    hi_s = result["summaries"]["high_rate_5pct"]
    hold_main = main_s["hold"]["geometric_annual_return"]
    base_main = main_s["f0001_1.0"]["geometric_annual_return"]
    base_hi = hi_s["f0001_1.0"]["geometric_annual_return"]

    rows = []
    for cfg in result["configs"]:
        sid = cfg["strategy_id"]
        m, r, h = main_s[sid], rob_s[sid], hi_s[sid]
        rows.append(
            [
                f"`{sid}`",
                cfg["label"],
                _pct(m["geometric_annual_return"]),
                _pts(m["geometric_annual_return"] - hold_main),
                _pts(m["geometric_annual_return"] - base_main),
                _pct(m["max_drawdown"]),
                _pct(m["annual_volatility"]),
                f"{m['excess_sharpe']:.2f}",
                f"{m['excess_calmar']:.2f}",
                _pct(r["geometric_annual_return"]),
                _pct(h["geometric_annual_return"]),
                _pts(h["geometric_annual_return"] - base_hi),
            ]
        )
    lines += _md_table(
        [
            "配置",
            "说明",
            "主算年化",
            "对 Hold",
            "对 1x",
            "最大回撤",
            "年化波动",
            "超额夏普",
            "超额卡玛",
            "稳健年化",
            "5%高息年化",
            "高息对 1x",
        ],
        rows,
    )

    lines += ["## 2. 四项拖累解剖表（主算口径，年化 %）", ""]
    anat_rows = []
    for cfg in result["configs"]:
        sid = cfg["strategy_id"]
        m = main_s[sid]
        anat_rows.append(
            [
                f"`{sid}`",
                _pct(m["arith_annual_return"]),
                _pct(m["log_annual_return"]),
                _pct(m["vol_drag_quadratic"]),
                _pct(m["vol_drag_realized"]),
                _pct(m["financing_drag_annual"]),
                _pct(m["turnover_drag_annual"]),
                f"{m['average_exposure']:.2f}",
                f"{m['average_borrowed']:.2f}",
                _pct(m["levered_day_share"]),
                f"{m['annual_turnover']:.2f}",
                f"{m['weekly_delever_triggers']}/{m['weekly_relever_triggers']}",
            ]
        )
    lines += _md_table(
        [
            "配置",
            "算术年化",
            "对数年化",
            "理论拖累(σ²/2)",
            "实际复利拖累",
            "融资利息拖累",
            "换手成本拖累",
            "平均敞口",
            "平均借钱",
            "开杠杆占比",
            "年换手",
            "周频去/开杠杆",
        ],
        anat_rows,
    )

    lines += ["## 3. 危机与长牛分段诊断（主算口径）", ""]
    sub_map = result["subperiods_main"]
    sub_rows = []
    for cfg in result["configs"]:
        sid = cfg["strategy_id"]
        parts = []
        for sp in sub_map.get(sid, []):
            flag = "已去杠杆" if sp["entry_delevered"] else f"带杠杆({sp['entry_exposure']:.2f})"
            parts.append(
                f"{_pct(sp['cumulative_return'])} / DD {_pct(sp['max_drawdown'])} ({flag})"
            )
        sub_rows.append([f"`{sid}`", *parts])
    sub_headers = ["配置", *[sp["label"] for sp in sub_map["hold"]]]
    lines += _md_table(sub_headers, sub_rows)

    lines += ["## 4. 开发窗口止损线逐项核对", ""]
    k_rows = []
    for item in result["verdict"]["details"]:
        c = item["checks"]
        d = item["deltas"]
        k_rows.append(
            [
                f"`{item['strategy_id']}`",
                "PASS" if c["c1_dd_not_worse_than_hold"] else "FAIL",
                f"{'PASS' if c['c2_beat_hold_main_1pp_and_robust_ge_0'] else 'FAIL'} "
                f"(主{_pts(d['main_vs_hold'])}/稳{_pts(d['robust_vs_hold'])})",
                f"{'PASS' if c['c3_beat_f0001_1x_main_and_5pct_rate'] else 'FAIL'} "
                f"(主{_pts(d['main_vs_f0001_1x'])}/高息{_pts(d['high_rate_5pct_vs_f0001_1x'])})",
                f"{'PASS' if c['c4_value_add_over_unfiltered'] else 'FAIL'} "
                f"(收益{_pts(d['main_vs_unfiltered'])}/DD{_pts(d['dd_vs_unfiltered'])})",
                "**PASS**" if item["pass_all"] else "**KILL**",
            ]
        )
    lines += _md_table(
        [
            "主测配置",
            "1.回撤≤Hold",
            "2.主算≥Hold+1且稳健≥Hold",
            "3.主算与5%高息均>1x",
            "4.优于同L无波动门",
            "总判",
        ],
        k_rows,
    )

    lines += [
        "---",
        "说明：本表仅为 `2001-02-28..2010-12-31` 开发窗口诊断，"
        "不构成验证窗口结论、投资建议或生产规则。",
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
    """TRADING-2573：纯 QQQ 非对称 VXN 波动门低倍择时杠杆开发窗口诊断。"""


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option(help="输入检查结果保存目录。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    try:
        data, _, _ = load_qqq_dev_data(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"asym_vxn_leverage: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None
    chk = check_inputs(data)
    write_check(chk, out)
    typer.echo(
        f"asym_vxn_leverage: QQQ {chk['return_days']} 天，VXN 缺失 {chk['vxn_missing_days']} 天，"
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
        typer.echo("asym_vxn_leverage: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        data, loaded, root_dir = load_qqq_dev_data(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"asym_vxn_leverage: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None
    result = run_probe(data, loaded, root_dir, provenance=git_provenance(PROJECT_ROOT))
    write_check(result["check"], out)
    if result["stopped"] is not None:
        typer.echo(result["stopped"]["text"])
        raise typer.Exit(code=1)
    write_result(result, out)
    typer.echo(f"asym_vxn_leverage: 判定 {result['verdict']['verdict']}；结果目录 {out}")


if __name__ == "__main__":
    app()
