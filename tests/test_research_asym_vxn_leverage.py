"""Acceptance tests for TRADING-2573 pure-QQQ asymmetric VXN leverage dev probe.

All inputs are synthetic: no real research snapshot or 2011+ data is opened.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_bench.data import BenchData, from_daily_frame
from ai_trading_system.research_probes import asym_vxn_leverage as avl
from ai_trading_system.research_probes import leverage_table as lt

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
PROVENANCE = {"commit": "test000", "dirty": False}


def _synthetic_qqq(
    end: str = "2012-12-31",
    *,
    seed: int = 7,
    vxn_fn: str = "regime",
    mutate_after_cutoff: bool = False,
) -> BenchData:
    index = pd.bdate_range("1999-03-10", end, name="date")
    rng = np.random.default_rng(seed)
    r = rng.normal(0.0004, 0.014, len(index))
    # Inject bear-like segments in 2001-2002 and 2008 so F0001 toggles cleanly.
    r[(index >= pd.Timestamp("2001-03-01")) & (index <= pd.Timestamp("2002-10-09"))] -= 0.0025
    r[(index >= pd.Timestamp("2008-01-02")) & (index <= pd.Timestamp("2009-03-09"))] -= 0.0030
    if mutate_after_cutoff:
        r[index > pd.Timestamp("2010-12-31")] = -0.15

    level = 100.0 * np.cumprod(1.0 + r)
    vxn = np.full(len(index), np.nan, dtype=float)
    valid = index >= pd.Timestamp("2001-02-02")
    if vxn_fn == "regime":
        vxn[valid] = 18.0
        vxn[(index >= pd.Timestamp("2001-02-02")) & (index <= pd.Timestamp("2003-03-31"))] = 34.0
        vxn[(index >= pd.Timestamp("2007-10-01")) & (index <= pd.Timestamp("2009-06-30"))] = 38.0
        # Mid-bull spike inside 2006 to trigger weekly deleveraging.
        vxn[(index >= pd.Timestamp("2006-05-10")) & (index <= pd.Timestamp("2006-06-20"))] = 28.0
    else:
        vxn[valid] = 19.0
    if mutate_after_cutoff:
        vxn[index > pd.Timestamp("2010-12-31")] = 99.0

    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "etf_reference_return": r,
            "cash_return": 0.00008,
            "lev2_return": 2.0 * r,
            "lev3_return": 3.0 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": np.where(np.isfinite(vxn), vxn * 0.85, 20.0),
            "vxn": vxn,
            "vxo": 20.0,
            "cash_rate_annual": 0.02,
        },
        index=index,
    )
    return from_daily_frame(
        daily, qqq_series="actual", source={"snapshot_id": "synthetic-qqq-test"}
    )


def test_cutoff_enforced_and_post_2010_mutation_ignored() -> None:
    clean = _synthetic_qqq(mutate_after_cutoff=False)
    mutated = _synthetic_qqq(mutate_after_cutoff=True)

    res_clean = avl.run_probe(clean, LOADED, ROOT_DIR, provenance=PROVENANCE)
    res_mutated = avl.run_probe(mutated, LOADED, ROOT_DIR, provenance=PROVENANCE)

    assert res_clean["stopped"] is None
    assert res_clean["check"]["max_loaded_date"] <= "2010-12-31"
    assert res_clean["summaries"] == res_mutated["summaries"]
    assert res_clean["verdict"] == res_mutated["verdict"]


def test_refuses_end_date_after_cutoff() -> None:
    data = _synthetic_qqq(end="2012-12-31")
    base = lt.f0001_targets(data, ROOT_DIR)
    cfg = avl.StrategyConfig("hold", "hold", "benchmark", "hold", 1.0)
    with pytest.raises(ValueError, match="2010-12-31"):
        avl.simulate_policy(
            data.returns,
            base,
            data.inputs["vxn"],
            cfg,
            avl.MAIN_COST,
            end=date(2011, 6, 30),
        )


def test_f0001_scaled_matches_leverage_table_exactly() -> None:
    data = _synthetic_qqq(end="2010-12-31")
    base = lt.f0001_targets(data, ROOT_DIR)
    for lev in (1.0, 1.3, 1.5):
        for cost, lt_cost in ((avl.MAIN_COST, lt.MAIN), (avl.ROBUST_COST, lt.ROBUST)):
            cfg = avl.StrategyConfig(f"f0001_{lev}", "ctrl", "benchmark", "f0001_scaled", lev)
            ours = avl.simulate_policy(data.returns, base, data.inputs["vxn"], cfg, cost)
            ref = lt.simulate_levered(
                data.returns,
                base,
                start=avl.DEV_START,
                end=avl.DEV_END,
                leverage=lev,
                costs=lt_cost,
            )
            np.testing.assert_allclose(
                ours.simulation.daily["return"].to_numpy(),
                ref.simulation.daily["return"].to_numpy(),
                atol=1e-12,
                rtol=0.0,
            )
            np.testing.assert_allclose(
                ours.simulation.daily["exposure"].to_numpy(),
                ref.simulation.daily["exposure"].to_numpy(),
                atol=1e-12,
                rtol=0.0,
            )


def test_hysteresis_and_nan_failsafe() -> None:
    assert avl._next_hysteresis(False, 20.0, 20.0, 25.0) is True
    assert avl._next_hysteresis(True, 22.0, 20.0, 25.0) is True
    assert avl._next_hysteresis(True, 25.0, 20.0, 25.0) is False
    assert avl._next_hysteresis(False, 22.0, 20.0, 25.0) is False
    assert avl._next_hysteresis(True, float("nan"), 20.0, 25.0) is False


def test_asymmetric_weekly_deleveraging_without_exiting_base() -> None:
    # Build a 4-month synthetic series where base is 1.0 on month-ends except a mid-month dip,
    # and VXN spikes to 30 on a mid-month Friday.
    index = pd.bdate_range("2004-12-01", "2005-04-29", name="date")
    returns = pd.DataFrame({"qqq": 0.001, "cash": 0.0001}, index=index)
    base = pd.Series(1.0, index=index)
    # Mid-March dip in base signal that recovers before March month-end: should NOT drop below 1.0.
    base.loc["2005-03-10":"2005-03-18"] = 0.0
    vxn = pd.Series(18.0, index=index)
    # Spike VXN on Friday 2005-03-11 (week-end, not month-end) -> executes Monday 2005-03-14 close,
    # takes effect on Tuesday 2005-03-15 return!
    vxn.loc["2005-03-11":"2005-03-24"] = 30.0

    asym_cfg = avl.StrategyConfig("asym", "asym", "primary", "asym_band", 1.5)
    monthly_cfg = avl.StrategyConfig("monthly", "monthly", "frequency_control", "monthly_band", 1.5)

    asym_run = avl.simulate_policy(
        returns, base, vxn, asym_cfg, avl.MAIN_COST, start=date(2005, 1, 31), end=date(2005, 4, 29)
    )
    mon_run = avl.simulate_policy(
        returns,
        base,
        vxn,
        monthly_cfg,
        avl.MAIN_COST,
        start=date(2005, 1, 31),
        end=date(2005, 4, 29),
    )

    # On 2005-03-15 (day after Monday execution of Friday 03-11 decision):
    # asym has delevered to 1.0 (NOT 0.0!), while monthly_band is still ~1.5x!
    assert asym_run.simulation.daily.loc[pd.Timestamp("2005-03-15"), "exposure"] == pytest.approx(
        1.0, abs=1e-9
    )
    assert mon_run.simulation.daily.loc[pd.Timestamp("2005-03-15"), "exposure"] > 1.4
    assert asym_run.weekly_delever_triggers == 1
    assert mon_run.weekly_delever_triggers == 0
    # Base never dropped below 1.0 because the mid-month dip recovered before month-end.
    assert float(asym_run.simulation.daily["exposure"].min()) >= 0.99


def test_invvol_forces_deleveraging_even_inside_deadband() -> None:
    index = pd.bdate_range("2004-12-01", "2005-03-31", name="date")
    returns = pd.DataFrame({"qqq": 0.0, "cash": 0.0}, index=index)
    base = pd.Series(1.0, index=index)
    # 25 / 24 = 1.0417 (only 0.0417 above 1.0, strictly smaller than 0.10 deadband).
    vxn = pd.Series(24.0, index=index)
    # On Friday 2005-02-11, VXN jumps to 26 -> desired=1.0; must still delever despite delta < 0.10.
    vxn.loc["2005-02-11":] = 26.0

    cfg = avl.StrategyConfig("inv", "inv", "smooth_control", "asym_invvol", 1.5)
    run = avl.simulate_policy(
        returns, base, vxn, cfg, avl.MAIN_COST, start=date(2005, 1, 31), end=date(2005, 3, 31)
    )
    assert run.simulation.daily.loc[pd.Timestamp("2005-02-15"), "exposure"] == pytest.approx(
        1.0, abs=1e-9
    )
    assert run.weekly_delever_triggers == 1


def test_high_rate_5pct_charges_fixed_borrow_only_when_levered() -> None:
    index = pd.bdate_range("2004-12-01", "2005-03-31", name="date")
    returns = pd.DataFrame({"qqq": 0.0, "cash": 0.0}, index=index)
    base = pd.Series(1.0, index=index)
    vxn = pd.Series(15.0, index=index)
    cfg_lev = avl.StrategyConfig("lev", "lev", "primary", "asym_band", 1.5)
    cfg_1x = avl.StrategyConfig("one", "one", "benchmark", "f0001_scaled", 1.0)

    r_lev = avl.simulate_policy(
        returns,
        base,
        vxn,
        cfg_lev,
        avl.HIGH_RATE_5PCT,
        start=date(2005, 1, 31),
        end=date(2005, 3, 31),
    )
    r_1x = avl.simulate_policy(
        returns,
        base,
        vxn,
        cfg_1x,
        avl.HIGH_RATE_5PCT,
        start=date(2005, 1, 31),
        end=date(2005, 3, 31),
    )
    # On 2005-02-02 (after 01-31 month-end executes at 02-01 close), held=1.5 -> borrowed=0.5
    # daily borrow cost = 0.5 * 0.05 / 252
    assert r_lev.simulation.daily.loc[pd.Timestamp("2005-02-02"), "financing_cost"] == (
        pytest.approx(0.5 * 0.05 / 252.0, abs=1e-12)
    )
    assert float(r_1x.simulation.daily["financing_cost"].sum()) == 0.0


def test_cli_check_run_markdown_and_no_overwrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    synth = _synthetic_qqq(end="2010-12-31")
    monkeypatch.setattr(avl, "load_qqq_dev_data", lambda _root: (synth, LOADED, ROOT_DIR))

    runner = CliRunner()
    out_dir = tmp_path / "probe_out"

    chk = runner.invoke(avl.app, ["check", "--out", str(out_dir)])
    assert chk.exit_code == 0, chk.output
    assert "PASS" in chk.output
    assert json.loads((out_dir / avl.CHECK_JSON).read_text(encoding="utf-8"))["ok"] is True

    run1 = runner.invoke(avl.app, ["run", "--out", str(out_dir)])
    assert run1.exit_code == 0, run1.output
    md = (out_dir / avl.RESULT_MD).read_text(encoding="utf-8")
    assert "TRADING-2573" in md
    assert "production_effect=none" in md
    assert "四项拖累解剖表" in md

    # Refuses to overwrite existing results.
    run2 = runner.invoke(avl.app, ["run", "--out", str(out_dir)])
    assert run2.exit_code == 1
    assert "拒绝覆盖" in run2.output
