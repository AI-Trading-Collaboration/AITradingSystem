"""Acceptance tests for TRADING-2574 pure-QQQ positive-convexity option proxy dev probe.

All inputs are synthetic: no real research snapshot or 2011+ data is opened.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_bench.data import BenchData, from_daily_frame
from ai_trading_system.research_probes import option_convexity_proxy as ocp
from ai_trading_system.research_probes import option_proxy as op

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
PROVENANCE = {"commit": "test000", "dirty": False}


def _synthetic_qqq(
    end: str = "2012-12-31",
    *,
    seed: int = 11,
    const_rate: float = 0.03,
    const_vxn: float | None = None,
    mutate_after_cutoff: bool = False,
) -> BenchData:
    index = pd.bdate_range("1999-03-10", end, name="date")
    rng = np.random.default_rng(seed)
    r = rng.normal(0.0004, 0.015, len(index))
    r[(index >= pd.Timestamp("2001-03-01")) & (index <= pd.Timestamp("2002-10-09"))] -= 0.0028
    r[(index >= pd.Timestamp("2008-01-02")) & (index <= pd.Timestamp("2009-03-09"))] -= 0.0032
    if mutate_after_cutoff:
        r[index > pd.Timestamp("2010-12-31")] = -0.20

    level = 100.0 * np.cumprod(1.0 + r)
    vxn = np.full(len(index), np.nan, dtype=float)
    valid = index >= pd.Timestamp("2001-02-02")
    if const_vxn is not None:
        vxn[valid] = const_vxn
    else:
        vxn[valid] = 22.0
        vxn[(index >= pd.Timestamp("2001-02-02")) & (index <= pd.Timestamp("2002-12-31"))] = 38.0
        vxn[(index >= pd.Timestamp("2008-01-02")) & (index <= pd.Timestamp("2009-06-30"))] = 42.0
    if mutate_after_cutoff:
        vxn[index > pd.Timestamp("2010-12-31")] = 95.0

    daily_cash = math.expm1(const_rate / ocp.TRADING_DAYS)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "etf_reference_return": r,
            "cash_return": daily_cash,
            "lev2_return": 2.0 * r,
            "lev3_return": 3.0 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": vxn,
            "vxo": 20.0,
            "cash_rate_annual": const_rate,
        },
        index=index,
    )
    return from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "synthetic-qqq-opt"})


def test_cutoff_enforced_and_post_2010_mutation_ignored() -> None:
    clean = _synthetic_qqq(mutate_after_cutoff=False)
    mutated = _synthetic_qqq(mutate_after_cutoff=True)

    res_clean = ocp.run_probe(clean, LOADED, ROOT_DIR, provenance=PROVENANCE)
    res_mutated = ocp.run_probe(mutated, LOADED, ROOT_DIR, provenance=PROVENANCE)

    assert res_clean["stopped"] is None
    assert res_clean["check"]["max_loaded_date"] <= "2010-12-31"
    assert res_clean["benchmarks"] == res_mutated["benchmarks"]
    assert res_clean["structures"] == res_mutated["structures"]
    assert res_clean["verdict"] == res_mutated["verdict"]


def test_put_call_parity_identity_for_cash_plus_call() -> None:
    # Under constant continuous rate r, (1 - c0) * e^{rt} + C_t(1) == S_t + P_t(1) - p0 * e^{rt}.
    data = _synthetic_qqq(end="2010-12-31", const_rate=0.04, const_vxn=25.0)
    inputs = ocp.ConvexityInputs.from_bench(data)
    spec = ocp.StructureSpec("c3", "c3", "call_replace", step_months=3, strike_primary=1.0)
    path = ocp.simulate_structure(inputs, spec, sigma_scale=1.0, notional_scale=1.0)

    index = pd.DatetimeIndex(inputs.vxn.index)
    c_start, c_end = ocp.multi_month_bounds(index, 3)[0]
    qqq = inputs.qqq_return.to_numpy(dtype=float)
    cash = inputs.cash_return.to_numpy(dtype=float)
    spot = np.concatenate([[1.0], np.cumprod(1.0 + qqq[c_start + 1 : c_end + 1])])
    grown = np.concatenate([[1.0], np.cumprod(1.0 + cash[c_start + 1 : c_end + 1])])
    years = (c_end - np.arange(c_start, c_end + 1)) / ocp.TRADING_DAYS
    p_vals = np.array(
        [op.bs_price("put", spot[k], 1.0, 0.04, 0.25, years[k]) for k in range(len(spot))]
    )
    w_parity = spot + p_vals - p_vals[0] * grown
    r_parity = w_parity[1:] / w_parity[:-1] - 1.0

    r_call = path.returns.iloc[: len(r_parity)].to_numpy()
    np.testing.assert_allclose(r_call, r_parity, atol=1e-9, rtol=0.0)


def test_call_notional_scaling_is_self_funded_while_put_uses_margin() -> None:
    data = _synthetic_qqq(end="2010-12-31", const_vxn=22.0)
    inputs = ocp.ConvexityInputs.from_bench(data)
    call_spec = ocp.StructureSpec("c6", "c6", "call_replace", step_months=6, strike_primary=1.0)
    put_spec = ocp.StructureSpec("p3", "p3", "protective_put", step_months=3, strike_primary=0.9)

    p_call = ocp.simulate_structure(inputs, call_spec, 1.0, notional_scale=1.5)
    p_put = ocp.simulate_structure(inputs, put_spec, 1.0, notional_scale=1.5)

    # 1.5x 6M ATM call premium is ~11% of wealth (< 100%), so zero margin borrowing!
    assert p_call.ever_borrowed is False
    assert p_put.ever_borrowed is True
    assert p_call.returns.index.equals(p_put.returns.index)


def test_decomposition_sum_and_buyer_sigma_monotonicity() -> None:
    data = _synthetic_qqq(end="2010-12-31", const_vxn=24.0)
    res = ocp.run_probe(data, LOADED, ROOT_DIR, provenance=PROVENANCE)

    for sid in ("call_3m_atm", "call_6m_atm", "call_6m_itm85", "put90_3m"):
        entry = res["structures"][sid]["by_scale"]
        g09 = entry["0.9"]["base_1x"]["geometric_annual_return"]
        g10 = entry["1.0"]["base_1x"]["geometric_annual_return"]
        g11 = entry["1.1"]["base_1x"]["geometric_annual_return"]
        # Paying higher implied volatility strictly lowers pure long-option return.
        assert g09 > g10 > g11

        dc = entry["1.0"]["decomposition"]
        assert dc["convexity_minus_premium"] + dc["delta_timing_value"] == pytest.approx(
            dc["total_vs_constant_mix"], abs=1e-12
        )


def test_cli_check_run_markdown_and_no_overwrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    synth = _synthetic_qqq(end="2010-12-31")
    monkeypatch.setattr(ocp, "load_qqq_dev_data", lambda _root: (synth, LOADED, ROOT_DIR))

    runner = CliRunner()
    out_dir = tmp_path / "opt_probe_out"

    chk = runner.invoke(ocp.app, ["check", "--out", str(out_dir)])
    assert chk.exit_code == 0, chk.output
    assert "PASS" in chk.output
    assert json.loads((out_dir / ocp.CHECK_JSON).read_text(encoding="utf-8"))["ok"] is True

    run1 = runner.invoke(ocp.app, ["run", "--out", str(out_dir)])
    assert run1.exit_code == 0, run1.output
    md = (out_dir / ocp.RESULT_MD).read_text(encoding="utf-8")
    assert "TRADING-2574" in md
    assert "Delta 副本拆账" in md
    assert "production_effect=none" in md

    run2 = runner.invoke(ocp.app, ["run", "--out", str(out_dir)])
    assert run2.exit_code == 1
    assert "拒绝覆盖" in run2.output
