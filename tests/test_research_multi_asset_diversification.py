"""Acceptance tests for TRADING-2575 multi-asset diversification dev probe.

All inputs are synthetic: no real snapshot, no network access, and no 2011+ data read.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_bench.data import BenchData, from_daily_frame, treasury_returns
from ai_trading_system.research_probes import asym_vxn_leverage as avl
from ai_trading_system.research_probes import leverage_table as lt
from ai_trading_system.research_probes import multi_asset_diversification as mad

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
PROVENANCE = {"commit": "test000", "dirty": False}


def _synthetic_universe(
    *,
    seed: int = 17,
    mutate_after_cutoff: bool = False,
    break_gold_overlap: bool = False,
) -> tuple[BenchData, dict[str, pd.Series]]:
    index = pd.bdate_range("1998-01-02", "2012-12-31", name="date")
    rng = np.random.default_rng(seed)

    r_qqq = rng.normal(0.0003, 0.016, len(index))
    r_qqq[(index >= pd.Timestamp("2000-03-27")) & (index <= pd.Timestamp("2002-10-09"))] -= 0.0028
    r_qqq[(index >= pd.Timestamp("2008-01-02")) & (index <= pd.Timestamp("2009-03-09"))] -= 0.0030
    if mutate_after_cutoff:
        r_qqq[index > pd.Timestamp("2010-12-31")] = -0.15

    r_gold = rng.normal(0.0004, 0.010, len(index)) - 0.15 * r_qqq
    r_oil = rng.normal(0.0002, 0.018, len(index))
    r_usd = rng.normal(0.0000, 0.005, len(index)) - 0.20 * r_gold

    # Synthetic 10Y and 20Y yields walking smoothly in [2.5, 6.5]%.
    y10 = np.clip(4.5 + np.cumsum(rng.normal(0.0, 0.04, len(index))), 2.0, 7.0)
    y20 = np.clip(y10 + 0.5 + rng.normal(0.0, 0.02, len(index)), 2.5, 7.5)

    level_qqq = 100.0 * np.cumprod(1.0 + r_qqq)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r_qqq,
            "etf_reference_return": r_qqq,
            "cash_return": 0.0001,
            "lev2_return": 2.0 * r_qqq,
            "lev3_return": 3.0 * r_qqq,
            "ndx_price": level_qqq,
            "ndx_tr": level_qqq,
            "vix": 20.0,
            "vxn": 24.0,
            "vxo": 20.0,
            "cash_rate_annual": 0.025,
        },
        index=index,
    )
    qqq_data = from_daily_frame(
        daily, qqq_series="actual", source={"snapshot_id": "synthetic-qqq-p2"}
    )

    # Build 2-A series on <= 2010-12-31.
    pre_cut = index <= pd.Timestamp("2010-12-31")
    idx_cut = index[pre_cut]
    gcusd = pd.Series(300.0 * np.cumprod(1.0 + r_gold[pre_cut]), index=idx_cut, name="GCUSD")
    dgs10 = pd.Series(y10[pre_cut], index=idx_cut, name="DGS10")
    dgs20 = pd.Series(y20[pre_cut], index=idx_cut, name="DGS20")
    oil = pd.Series(20.0 * np.cumprod(1.0 + r_oil[pre_cut]), index=idx_cut, name="DCOILWTICO")
    usd = pd.Series(100.0 * np.cumprod(1.0 + r_usd[pre_cut]), index=idx_cut, name="DTWEXM")

    # Build 2-B ETFs starting on their true inception dates with tiny realistic tracking noise.
    gld_idx = idx_cut[idx_cut >= pd.Timestamp("2004-11-18")]
    gld_r = r_gold[pre_cut][idx_cut >= pd.Timestamp("2004-11-18")]
    if break_gold_overlap:
        gld_r = rng.normal(0.0, 0.02, len(gld_idx))
    else:
        gld_r = gld_r - 0.004 / 252.0 + rng.normal(0.0, 0.0005, len(gld_idx))
    gld = pd.Series(45.0 * np.cumprod(1.0 + gld_r), index=gld_idx, name="GLD")

    ief_idx = idx_cut[idx_cut >= pd.Timestamp("2002-07-30")]
    b10 = mad.par_bond_returns(dgs10, idx_cut, years_maturity=10).loc[ief_idx].fillna(0.0)
    ief_r = b10.to_numpy() - 0.0015 / 252.0 + rng.normal(0.0, 0.0004, len(ief_idx))
    ief = pd.Series(80.0 * np.cumprod(1.0 + ief_r), index=ief_idx, name="IEF")

    b20 = mad.par_bond_returns(dgs20, idx_cut, years_maturity=20).loc[ief_idx].fillna(0.0)
    tlt_r = b20.to_numpy() - 0.0015 / 252.0 + rng.normal(0.0, 0.0008, len(ief_idx))
    tlt = pd.Series(85.0 * np.cumprod(1.0 + tlt_r), index=ief_idx, name="TLT")

    raw = {
        "GLD": gld,
        "IEF": ief,
        "TLT": tlt,
        "GCUSD": gcusd,
        "DGS10": dgs10,
        "DGS20": dgs20,
        "DCOILWTICO": oil,
        "DTWEXM": usd,
    }
    return qqq_data, raw


def test_par_bond_10y_matches_research_bench_exactly() -> None:
    _, raw = _synthetic_universe()
    cal = pd.DatetimeIndex(raw["DGS10"].index)
    ours = mad.par_bond_returns(raw["DGS10"], cal, years_maturity=10)
    ref = treasury_returns(raw["DGS10"], cal)
    np.testing.assert_allclose(ours.dropna().to_numpy(), ref.dropna().to_numpy(), atol=1e-12)


def test_overlap_gate_fails_closed_when_gold_diverges() -> None:
    qqq_data, bad_raw = _synthetic_universe(break_gold_overlap=True)
    res = mad.run_probe(qqq_data, bad_raw, ROOT_DIR, provenance=PROVENANCE)
    assert res["stopped"] is not None
    assert res["check"]["overlap_ok"] is False
    assert res["check"]["overlap_pairs"][0]["ok"] is False


def test_cutoff_enforced_and_post_2010_mutation_ignored() -> None:
    q_clean, raw = _synthetic_universe(mutate_after_cutoff=False)
    q_mut, _ = _synthetic_universe(mutate_after_cutoff=True)

    r1 = mad.run_probe(q_clean, raw, ROOT_DIR, provenance=PROVENANCE)
    r2 = mad.run_probe(q_mut, raw, ROOT_DIR, provenance=PROVENANCE)

    assert r1["stopped"] is None
    assert r1["check"]["overlap_ok"] is True
    assert r1["check"]["max_loaded_date"] <= "2010-12-31"
    assert r1["windows"] == r2["windows"]
    assert r1["verdict"] == r2["verdict"]


def test_single_asset_f0001_matches_leverage_table_exactly() -> None:
    qqq_data, raw = _synthetic_universe()
    frame, audit = mad.build_spliced_returns(qqq_data, raw)
    assert audit["ok"] is True
    base = lt.f0001_targets(qqq_data, ROOT_DIR)
    spec = mad.PortfolioSpec("f0001_1.0", "f0001", "f0001", is_candidate=False)
    targets = mad.compute_target_weights(frame, base, spec)

    ours = mad.simulate_portfolio(
        frame, targets, avl.MAIN_COST, start=mad.FULL_DEV_START, end=mad.DEV_CUTOFF, leverage=1.0
    )
    ref = lt.simulate_levered(
        frame[["qqq", "cash"]],
        base.reindex(frame.index),
        start=mad.FULL_DEV_START,
        end=mad.DEV_CUTOFF,
        leverage=1.0,
        costs=lt.MAIN,
    )
    np.testing.assert_allclose(
        ours.returns.to_numpy(), ref.simulation.daily["return"].to_numpy(), atol=1e-12
    )


def test_snapshot_checksum_and_cutoff_refusal(tmp_path: Path) -> None:
    snap = tmp_path / "snap"
    snap.mkdir()
    csv_bytes = b"date,series,value\n2010-12-31,GLD,100.0\n"
    snap.joinpath(mad.SERIES_CSV).write_bytes(csv_bytes)
    snap.joinpath(mad.MANIFEST_JSON).write_text(
        json.dumps({"series_csv_sha256": "deadbeef"}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="SHA-256"):
        mad.load_multi_asset_snapshot(snap)

    # Also refuses post-2010 date even with valid checksum.
    bad_bytes = b"date,series,value\n2011-01-03,GLD,100.0\n"
    snap.joinpath(mad.SERIES_CSV).write_bytes(bad_bytes)
    snap.joinpath(mad.MANIFEST_JSON).write_text(
        json.dumps({"series_csv_sha256": mad._sha256_bytes(bad_bytes)}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="2010-12-31"):
        mad.load_multi_asset_snapshot(snap)


def test_cli_check_run_and_no_overwrite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    qqq_data, raw = _synthetic_universe()
    monkeypatch.setattr(mad, "load_qqq_dev_data", lambda _root: (qqq_data, LOADED, ROOT_DIR))
    monkeypatch.setattr(mad, "load_multi_asset_snapshot", lambda _dir: (raw, {}))

    runner = CliRunner()
    out_dir = tmp_path / "p2_out"
    snap_dir = tmp_path / "unused_snap"

    chk = runner.invoke(mad.app, ["check", "--snapshot", str(snap_dir), "--out", str(out_dir)])
    assert chk.exit_code == 0, chk.output
    assert "PASS" in chk.output

    run1 = runner.invoke(mad.app, ["run", "--snapshot", str(snap_dir), "--out", str(out_dir)])
    assert run1.exit_code == 0, run1.output
    md = (out_dir / mad.RESULT_MD).read_text(encoding="utf-8")
    assert "TRADING-2575" in md
    assert "gold_gld_vs_gcusd" in md
    assert "production_effect=none" in md

    run2 = runner.invoke(mad.app, ["run", "--snapshot", str(snap_dir), "--out", str(out_dir)])
    assert run2.exit_code == 1
    assert "拒绝覆盖" in run2.output
