"""Acceptance tests for TRADING-2575 one-shot 2011-2021 validation probe.

All inputs are synthetic: no real snapshot, no network access, and zero 2021-02-22+ data read.
"""

from __future__ import annotations

import hashlib
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
from ai_trading_system.research_probes import multi_asset_diversification as mad
from ai_trading_system.research_probes import multi_asset_validation_2011 as val

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
PROVENANCE = {"commit": "test2575val", "dirty": False}


def _synthetic_val_universe(
    *,
    seed: int = 23,
    mutate_after_val_end: bool = False,
    gold_drift: float = -0.0001,
    bond_drift: float = 0.00015,
) -> tuple[BenchData, dict[str, pd.Series]]:
    index = pd.bdate_range("2009-10-01", "2022-06-30", name="date")
    rng = np.random.default_rng(seed)

    r_qqq = rng.normal(0.00055, 0.012, len(index))
    r_qqq[(index >= pd.Timestamp("2020-02-20")) & (index <= pd.Timestamp("2020-03-23"))] -= 0.015
    if mutate_after_val_end:
        r_qqq[index > pd.Timestamp("2021-02-19")] = -0.20

    r_gold = rng.normal(gold_drift, 0.009, len(index)) - 0.10 * r_qqq
    r_ief = rng.normal(bond_drift, 0.004, len(index)) - 0.25 * r_qqq

    level_qqq = 100.0 * np.cumprod(1.0 + r_qqq)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r_qqq,
            "etf_reference_return": r_qqq,
            "cash_return": 0.00003,
            "lev2_return": 2.0 * r_qqq,
            "lev3_return": 3.0 * r_qqq,
            "ndx_price": level_qqq,
            "ndx_tr": level_qqq,
            "vix": 16.0,
            "vxn": 19.0,
            "vxo": 16.0,
            "cash_rate_annual": 0.008,
        },
        index=index,
    )
    qqq_data = from_daily_frame(
        daily, qqq_series="actual", source={"snapshot_id": "synthetic-qqq-val2011"}
    )

    pre_cut = index <= pd.Timestamp(val.VAL_END)
    idx_cut = index[pre_cut]
    gld = pd.Series(100.0 * np.cumprod(1.0 + r_gold[pre_cut]), index=idx_cut, name="GLD")
    ief = pd.Series(90.0 * np.cumprod(1.0 + r_ief[pre_cut]), index=idx_cut, name="IEF")
    return qqq_data, {"GLD": gld, "IEF": ief}


def test_hard_cutoff_refuses_post_2021_02_19_and_invariance() -> None:
    qqq_clean, raw = _synthetic_val_universe(seed=41, mutate_after_val_end=False)
    qqq_mut, _ = _synthetic_val_universe(seed=41, mutate_after_val_end=True)

    with pytest.raises(ValueError, match="2021-02-19"):
        val.fetch_validation_snapshot(
            Path("unused"),
            price_provider=None,
            end=date(2021, 2, 22),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="2021-02-19"):
        val.build_validation_frame(qqq_clean, raw, end=date(2021, 2, 22))

    res_clean = val.run_validation(
        qqq_clean, raw, ROOT_DIR, provenance=PROVENANCE, bootstrap_draws=50
    )
    res_mut = val.run_validation(qqq_mut, raw, ROOT_DIR, provenance=PROVENANCE, bootstrap_draws=50)

    assert res_clean["stopped"] is None
    assert res_clean["check"]["max_loaded_date"] == "2021-02-19"
    assert res_clean["verdict"]["verdict"] == res_mut["verdict"]["verdict"]
    for sid in res_clean["strategies"]:
        assert res_clean["strategies"][sid]["main"]["geometric_annual_return"] == pytest.approx(
            res_mut["strategies"][sid]["main"]["geometric_annual_return"]
        )
        assert res_clean["strategies"][sid]["main"]["max_drawdown"] == pytest.approx(
            res_mut["strategies"][sid]["main"]["max_drawdown"]
        )


def test_snapshot_sha256_tamper_and_date_cutoff_refused(tmp_path: Path) -> None:
    good = pd.DataFrame(
        {
            "date": ["2010-01-04", "2021-02-19", "2010-01-04", "2021-02-19"],
            "series": ["GLD", "GLD", "IEF", "IEF"],
            "value": [109.8, 166.9, 88.5, 117.2],
        }
    )
    payload = good.to_csv(index=False, lineterminator="\n").encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    snap = tmp_path / "snap"
    snap.mkdir()
    (snap / val.SERIES_CSV).write_bytes(payload)
    (snap / val.MANIFEST_JSON).write_text(
        json.dumps({"series_csv_sha256": digest}), encoding="utf-8"
    )

    loaded, _ = val.load_validation_snapshot(snap)
    assert set(loaded.keys()) == {"GLD", "IEF"}

    # Tamper CSV -> SHA-256 mismatch.
    (snap / val.SERIES_CSV).write_bytes(payload + b"2021-02-19,GLD,999.0\n")
    with pytest.raises(ValueError, match="SHA-256"):
        val.load_validation_snapshot(snap)

    # Valid SHA-256 but includes 2021-02-22 -> cutoff refused.
    bad_date = pd.concat(
        [good, pd.DataFrame({"date": ["2021-02-22"], "series": ["GLD"], "value": [168.0]})],
        ignore_index=True,
    )
    bad_bytes = bad_date.to_csv(index=False, lineterminator="\n").encode("utf-8")
    (snap / val.SERIES_CSV).write_bytes(bad_bytes)
    (snap / val.MANIFEST_JSON).write_text(
        json.dumps({"series_csv_sha256": hashlib.sha256(bad_bytes).hexdigest()}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="exceeds hard cutoff"):
        val.load_validation_snapshot(snap)


def test_dev_2010_overlap_detects_fmp_drift() -> None:
    qqq_data, raw = _synthetic_val_universe(seed=7)
    dev_ok = {
        "GLD": raw["GLD"].loc[raw["GLD"].index <= pd.Timestamp("2010-12-31")].copy(),
        "IEF": raw["IEF"].loc[raw["IEF"].index <= pd.Timestamp("2010-12-31")].copy(),
    }
    _, audit_ok = val.build_validation_frame(qqq_data, raw, dev_raw_series=dev_ok)
    assert audit_ok["ok"] is True
    assert audit_ok["dev_overlap_2010"]["ok"] is True

    # Inject a 0.5% single-day return jump into 2010 GLD in dev snapshot.
    dev_drift = {k: v.copy() for k, v in dev_ok.items()}
    dev_drift["GLD"].loc[dev_drift["GLD"].index >= pd.Timestamp("2010-06-15")] *= 1.005
    res_fail = val.run_validation(
        qqq_data,
        raw,
        ROOT_DIR,
        provenance=PROVENANCE,
        dev_raw_series=dev_drift,
        bootstrap_draws=20,
    )
    assert res_fail["check"]["ok"] is False
    assert res_fail["check"]["dev_overlap_2010"]["ok"] is False
    assert res_fail["stopped"]["reason"] == "validation_data_check_failed"


def test_exact_1_5x_weights_and_trend_shield_borrow_reduction() -> None:
    # Deterministic bear trend on both gold and bond so trend shield drops both legs to cash.
    index = pd.bdate_range("2009-10-01", "2021-02-19", name="date")
    r_qqq = np.full(len(index), 0.0004)
    r_gold = np.full(len(index), -0.0002)
    r_ief = np.full(len(index), -0.0001)
    level_qqq = 100.0 * np.cumprod(1.0 + r_qqq)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r_qqq,
            "etf_reference_return": r_qqq,
            "cash_return": 0.00004,
            "lev2_return": 2.0 * r_qqq,
            "lev3_return": 3.0 * r_qqq,
            "ndx_price": level_qqq,
            "ndx_tr": level_qqq,
            "vix": 15.0,
            "vxn": 18.0,
            "vxo": 15.0,
            "cash_rate_annual": 0.01,
        },
        index=index,
    )
    qqq_data = from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "det"})
    raw = {
        "GLD": pd.Series(100.0 * np.cumprod(1.0 + r_gold), index=index, name="GLD"),
        "IEF": pd.Series(100.0 * np.cumprod(1.0 + r_ief), index=index, name="IEF"),
    }
    frame, audit = val.build_validation_frame(qqq_data, raw)
    assert audit["ok"] is True

    dummy_f1 = pd.Series(1.0, index=frame.index)
    t_static = mad.compute_target_weights(
        frame, dummy_f1, mad.PortfolioSpec("s", "s", "qqq60_ief20_gold20")
    )
    t_trend = mad.compute_target_weights(
        frame, dummy_f1, mad.PortfolioSpec("t", "t", "qqq60_trend_shield40")
    )

    sim_static = val.simulate_validation_portfolio(frame, t_static, avl.MAIN_COST, leverage=1.50)
    sim_trend = val.simulate_validation_portfolio(frame, t_trend, avl.MAIN_COST, leverage=1.50)

    # Static 1.5x holds 0.90 QQQ + 0.30 IEF + 0.30 GLD -> gross ~ 1.50, borrowed ~ 0.50.
    assert float(sim_static.gross_exposure.mean()) == pytest.approx(1.50, abs=0.02)
    assert float(sim_static.net_borrowed.mean()) == pytest.approx(0.50, abs=0.02)

    # Trend 1.5x drops negative gold & bond -> holds only 0.90 QQQ + 0.10 cash -> borrowed == 0.
    assert float(sim_trend.gross_exposure.mean()) == pytest.approx(0.90, abs=0.02)
    assert float(sim_trend.net_borrowed.mean()) == pytest.approx(0.0, abs=1e-9)


def test_preregistered_verdict_pass_and_fail_only_judges_primary() -> None:
    cands = val.default_candidates()

    def _mk(geo_m: float, geo_r: float, dd: float, sr: float) -> dict[str, dict[str, float]]:
        return {
            "main": {
                "geometric_annual_return": geo_m,
                "max_drawdown": dd,
                "excess_sharpe": sr,
            },
            "robust": {"geometric_annual_return": geo_r},
            "high_rate_5pct": {"geometric_annual_return": geo_m - 0.01},
        }

    # Case 1: Primary 1 passes all 3, Primary 2 fails return -> pass_validation.
    st_pass = {
        "hold": _mk(0.20, 0.20, -0.28, 1.00),
        "stack_static_1.5": _mk(0.205, 0.190, -0.285, 1.08),
        "stack_trend_1.5": _mk(0.195, 0.180, -0.26, 1.05),
        "qqq60_ief20_gold20_1.0": _mk(0.14, 0.14, -0.18, 1.10),
        "qqq60_trend_shield40_1.0": _mk(0.13, 0.13, -0.17, 1.05),
        "qqq70_gold30_1.35": _mk(0.25, 0.24, -0.25, 1.20),
        "f0001_1.0": _mk(0.18, 0.18, -0.20, 1.02),
    }
    v1 = val.evaluate_validation_verdict(st_pass, cands)
    assert v1["verdict"] == "pass_validation"
    assert v1["passed_ids"] == ["stack_static_1.5"]

    # Case 2: Both primary fail (one on DD > 1pp deeper, one on robust gap > 1.5pp),
    # even though control qqq70_gold30_1.35 crushes hold -> MUST be fail_validation.
    st_fail = {
        **st_pass,
        "stack_static_1.5": _mk(0.21, 0.20, -0.295, 1.10),  # DD 1.5pp deeper than hold -> fail c1
        "stack_trend_1.5": _mk(0.201, 0.180, -0.27, 1.05),  # robust 2.0pp behind hold -> fail c2
    }
    v2 = val.evaluate_validation_verdict(st_fail, cands)
    assert v2["verdict"] == "fail_validation"
    assert v2["passed_ids"] == []


def test_cli_one_shot_refuses_overwrite_and_markdown_renders(tmp_path: Path) -> None:
    qqq_data, raw = _synthetic_val_universe(seed=11)
    res = val.run_validation(qqq_data, raw, ROOT_DIR, provenance=PROVENANCE, bootstrap_draws=30)
    md = val.render_markdown(res)
    assert "TRADING-2575" in md
    assert "2021-02-19" in md
    assert "`production_effect=none`" in md
    assert "stack_static_1.5" in md
    assert "stack_trend_1.5" in md

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / val.RESULT_MD).write_text("existing\n", encoding="utf-8")
    runner = CliRunner()
    cli_res = runner.invoke(
        val.app, ["run", "--snapshot", str(tmp_path / "snap"), "--out", str(out_dir)]
    )
    assert cli_res.exit_code != 0
    assert "拒绝覆盖" in cli_res.output
