from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_bench import (
    Rebalance,
    RunSpec,
    WindowNotOpenError,
    WindowPermit,
    from_daily_frame,
    from_snapshot,
    load_settings,
    run_strategy,
    simulate,
)
from ai_trading_system.research_bench import __main__ as cli
from ai_trading_system.research_bench.metrics import (
    compare,
    cvar,
    cvar_field,
    exposure_gap,
    summarize,
)
from ai_trading_system.research_bench.settings import resolve_window
from ai_trading_system.research_bench.simulate import (
    exposure_weights,
    schedule_mask,
    weights_for_exposure,
)
from ai_trading_system.research_data import qqq_long_history as qlh

LOADED = load_settings()


def _daily(index: pd.DatetimeIndex, qqq: list[float] | np.ndarray[Any, Any]) -> pd.DataFrame:
    r = np.asarray(qqq, dtype=float)
    level = 100.0 * np.cumprod(1.0 + np.nan_to_num(r))
    return pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "etf_reference_return": r + 0.0001,
            "cash_return": np.zeros(len(index)),
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": np.nan,
            "cash_rate_annual": np.linspace(0.01, 0.02, len(index)),
        },
        index=index,
    )


def _returns(qqq: list[float], cash: float = 0.0, start: str = "2001-01-01") -> pd.DataFrame:
    index = pd.bdate_range(start, periods=len(qqq), name="date")
    r = np.asarray(qqq, dtype=float)
    return pd.DataFrame(
        {"qqq": r, "cash": np.full(len(r), cash), "lev2": 2 * r, "lev3": 3 * r}, index=index
    )


def test_settings_use_the_unified_cost_model_and_fixed_episodes() -> None:
    assert LOADED.cost_bps == 4.0
    assert LOADED.settings.cvar_levels == (0.01, 0.05)
    ids = [episode.id for episode in LOADED.settings.episodes]
    assert ids[0] == "dotcom_bear" and ids[-1] == "inflation_bear_2022"
    assert [cvar_field(level) for level in LOADED.settings.cvar_levels] == [
        "cvar_1pct",
        "cvar_5pct",
    ]


def test_windows_come_from_the_policy_and_registry() -> None:
    development = resolve_window(LOADED, "qqq_direction", "development")
    validation = resolve_window(LOADED, "qqq_direction", "validation")
    consistency = resolve_window(LOADED, "qqq_direction", "consistency_check")
    assert (development.start, development.end) == (date(1999, 3, 10), date(2010, 12, 31))
    assert (validation.start, validation.end) == (date(2011, 1, 3), date(2021, 2, 19))
    assert (consistency.start, consistency.end) == (date(2021, 2, 22), None)
    with pytest.raises(KeyError, match="no question type"):
        resolve_window(LOADED, "not_a_question", "development")


def test_exposure_maps_to_instruments_that_can_be_bought() -> None:
    assert weights_for_exposure(0.25) == [0.25, 0.75, 0.0, 0.0]
    assert weights_for_exposure(1.5) == [0.5, 0.0, 0.5, 0.0]
    assert weights_for_exposure(2.75) == [0.0, 0.0, 0.25, 0.75]
    frame = exposure_weights(pd.Series([0.0, 1.0, 2.0, 3.0]))
    assert frame.to_numpy().tolist() == [
        [0.0, 1.0, 0.0, 0.0],
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ]
    for bad in (-0.1, 3.01, math.nan):
        with pytest.raises(ValueError, match="target exposure"):
            weights_for_exposure(bad)


def test_schedules_read_the_last_date_of_each_week_or_month() -> None:
    index = pd.DatetimeIndex(
        ["2001-01-29", "2001-01-30", "2001-01-31", "2001-02-01", "2001-02-02", "2001-02-05"]
    )
    assert schedule_mask(index, "daily") == [True] * 6
    assert schedule_mask(index, "month_end") == [False, False, True, False, False, True]
    assert schedule_mask(index, "week_end") == [False, False, False, False, True, True]
    with pytest.raises(ValueError, match="schedule"):
        Rebalance("yearly")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="band"):
        Rebalance("daily", band=1.0)


def test_simulation_drifts_rebalances_and_charges_cost_on_fixed_numbers() -> None:
    returns = _returns([math.nan, 0.10, -0.10, 0.05, 0.0, 0.02])
    index = returns.index
    targets = pd.Series([0.5, 0.5, 1.0, 1.0, 1.0, 1.0], index=index)
    sim = simulate(
        returns, targets, start=index[1].date(), end=None, rebalance=Rebalance(), cost_bps=10.0
    )
    daily = sim.daily
    assert sim.start == index[1]
    assert list(daily.index) == list(index[2:])
    # Day 2: half QQQ (-10%), half cash; back to 50/50 at the close (decision of day 1).
    drift_qqq = 0.5 * 0.9 / 0.95
    turnover_2 = (abs(0.5 - drift_qqq) + abs(0.5 - (1 - drift_qqq))) / 2
    assert daily["gross_return"].iloc[0] == pytest.approx(-0.05)
    assert daily["turnover"].iloc[0] == pytest.approx(turnover_2)
    assert daily["return"].iloc[0] == pytest.approx(0.95 * (1 - turnover_2 * 0.001) - 1)
    # Day 3: still 50/50 (+5%), then all QQQ at the close (decision of day 2).
    drift_qqq = 0.5 * 1.05 / 1.025
    turnover_3 = ((1 - drift_qqq) + (1 - drift_qqq)) / 2
    assert daily["gross_return"].iloc[1] == pytest.approx(0.025)
    assert daily["turnover"].iloc[1] == pytest.approx(turnover_3)
    assert daily["exposure"].tolist() == pytest.approx([0.5, 0.5, 1.0, 1.0])
    assert daily["return"].iloc[3] == pytest.approx(0.02)


def test_band_skips_small_deviations_and_month_end_ignores_other_dates() -> None:
    returns = _returns([math.nan] + [0.01] * 59)
    index = returns.index
    targets = pd.Series(0.5, index=index)
    banded = simulate(
        returns,
        targets,
        start=index[1].date(),
        end=None,
        rebalance=Rebalance("daily", band=0.2),
        cost_bps=0.0,
    )
    exposure = banded.exposure.to_numpy()
    # Rising QQQ lifts the drifted exposure; it is reset only once it is 20% above 0.5.
    assert exposure.max() <= 0.6 + 0.01
    assert (banded.daily["turnover"] > 0).sum() < 10
    month_ends = [i for i, flag in enumerate(schedule_mask(index, "month_end")) if flag]
    january, february = month_ends[0], month_ends[1]
    targets.iloc[january + 5 :] = 1.0  # decided mid-February: acted on only at its last date
    monthly = simulate(
        returns,
        targets,
        start=index[january + 1].date(),
        end=None,
        rebalance=Rebalance("month_end"),
        cost_bps=0.0,
    )
    assert monthly.exposure.loc[index[february + 1]] < 1.0
    assert monthly.exposure.loc[index[february + 2]] == pytest.approx(1.0)


def test_simulation_refuses_bad_targets_missing_returns_and_short_lookback() -> None:
    returns = _returns([math.nan, 0.01, 0.02, 0.03])
    index = returns.index
    with pytest.raises(ValueError, match="executable"):
        simulate(
            returns,
            pd.Series(1.0, index=index),
            start=index[0].date(),
            end=None,
            rebalance=Rebalance(),
            cost_bps=0.0,
        )
    with pytest.raises(ValueError, match="2001-01-03: target exposure"):
        simulate(
            returns,
            pd.Series([1.0, 1.0, 4.0, 1.0], index=index),
            start=index[1].date(),
            end=None,
            rebalance=Rebalance(),
            cost_bps=0.0,
        )
    gappy = returns.copy()
    gappy.iloc[2, 3] = math.nan
    with pytest.raises(ValueError, match="lev3 has no return on 2001-01-03"):
        simulate(
            gappy,
            pd.Series(1.0, index=index),
            start=index[1].date(),
            end=None,
            rebalance=Rebalance(),
            cost_bps=0.0,
        )


def test_metrics_on_fixed_numbers() -> None:
    returns = _returns([math.nan, 0.10, -0.20, 0.05, 0.10, -0.05, 0.0], start="2004-01-05")
    index = returns.index
    sim = simulate(
        returns,
        pd.Series(1.0, index=index),
        start=index[1].date(),
        end=None,
        rebalance=Rebalance(),
        cost_bps=0.0,
    )
    summary = summarize(sim, returns["cash"], LOADED)
    path = [-0.20, 0.05, 0.10, -0.05, 0.0]
    equity = np.cumprod(1 + np.array(path))
    assert summary["max_drawdown"] == pytest.approx(-0.20)
    assert summary["longest_drawdown_days"] == 5
    assert summary["cvar_1pct"] == pytest.approx(-0.20)
    assert summary["cvar_5pct"] == pytest.approx(-0.20)
    assert cvar(pd.Series(path), 0.5) == pytest.approx((-0.20 - 0.05 + 0.0) / 3)
    years = (index[-1] - index[1]).days / 365.25
    assert summary["geometric_annual_return"] == pytest.approx(equity[-1] ** (1 / years) - 1)
    assert summary["worst_month"] == pytest.approx(equity[-1] - 1)
    assert summary["rolling_geometric_return_low"] is None
    assert summary["average_exposure"] == 1.0
    assert summary["episodes"] == {}


def test_episodes_report_complete_and_partial_ones() -> None:
    index = pd.bdate_range("2007-06-01", "2009-02-27", name="date")
    returns = pd.DataFrame({"qqq": 0.001, "cash": 0.0, "lev2": 0.002, "lev3": 0.003}, index=index)
    sim = simulate(
        returns,
        pd.Series(1.0, index=index),
        start=index[1].date(),
        end=None,
        rebalance=Rebalance(),
        cost_bps=0.0,
    )
    episodes = summarize(sim, returns["cash"], LOADED)["episodes"]
    assert list(episodes) == ["global_financial_crisis"]
    assert episodes["global_financial_crisis"]["complete"] is False
    days = int(((index >= "2007-10-01") & (index <= "2009-03-31")).sum())
    assert episodes["global_financial_crisis"]["return"] == pytest.approx(1.001**days - 1)


def test_compare_and_exposure_gap_signs() -> None:
    strategy = {"cvar_1pct": -0.03, "cvar_5pct": -0.02, "max_drawdown": -0.3}
    benchmark = {"cvar_1pct": -0.04, "cvar_5pct": -0.025, "max_drawdown": -0.5}
    out = compare(strategy, benchmark, LOADED)
    assert out["max_drawdown"] == pytest.approx(0.2)
    assert out["cvar_5pct_relative_improvement"] == pytest.approx(0.2)
    assert out["geometric_annual_return"] is None
    returns = _returns([math.nan, 0.02, -0.03, 0.01])
    index = returns.index
    sim = simulate(
        returns,
        pd.Series([0.5, 0.5, 0.5, 0.5], index=index),
        start=index[1].date(),
        end=None,
        rebalance=Rebalance(),
        cost_bps=0.0,
    )
    gap = exposure_gap(sim, 1.0, returns["qqq"])
    years = (index[-1] - index[1]).days / 365.25
    assert gap["missed_upside"] == pytest.approx(0.5 * 0.01 / years)
    assert gap["avoided_downside"] == pytest.approx(0.5 * 0.03 / years)
    assert gap["extra_upside"] == gap["extra_downside"] == 0.0


def test_daily_frame_adapter_picks_the_qqq_series_and_lags_the_rate() -> None:
    index = pd.bdate_range("2001-01-01", periods=5, name="date")
    daily = _daily(index, [math.nan, 0.01, 0.02, -0.01, 0.0])
    proxy = from_daily_frame(daily, qqq_series="proxy", source={"snapshot_id": "s"})
    actual = from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "s"})
    assert list(proxy.returns.columns) == ["qqq", "cash", "lev2", "lev3"]
    assert proxy.returns["qqq"].iloc[1] == pytest.approx(0.01)
    assert actual.returns["qqq"].iloc[1] == pytest.approx(0.0101)
    assert actual.source == {"snapshot_id": "s", "qqq_series": "actual"}
    assert math.isnan(proxy.inputs["cash_rate_known"].iloc[0])
    assert proxy.inputs["cash_rate_known"].iloc[1] == pytest.approx(0.01)
    with pytest.raises(ValueError, match="missing columns"):
        from_daily_frame(daily.drop(columns=["vxo"]), qqq_series="proxy", source={})
    with pytest.raises(ValueError, match="qqq_series"):
        from_daily_frame(daily, qqq_series="other", source={})  # type: ignore[arg-type]


def test_snapshot_adapter_carries_the_check_status_and_warnings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    index = pd.bdate_range("2001-01-01", periods=5, name="date")
    report = {
        "status": "WARN",
        "snapshot_id": "20200630-abc",
        "series_csv_sha256": "c" * 64,
        "spec_sha256": "s" * 64,
        "issues": [
            {"severity": "WARN", "code": "calendar_gap"},
            {"severity": "WARN", "code": "calendar_gap"},
            {"severity": "INFO", "code": "note"},
        ],
    }
    calls: list[Path] = []

    def fake_load(snapshot_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
        calls.append(snapshot_dir)
        return _daily(index, [math.nan, 0.01, 0.02, -0.01, 0.0]), report

    monkeypatch.setattr(qlh, "load_daily", fake_load)
    data = from_snapshot(tmp_path, qqq_series="proxy")
    assert calls == [tmp_path]
    assert data.source["snapshot_id"] == "20200630-abc"
    assert data.source["check_status"] == "WARN"
    assert data.source["check_warnings"] == ["calendar_gap"]


def _synthetic_daily() -> pd.DataFrame:
    index = pd.bdate_range("1997-01-02", "2011-06-30", name="date")
    rng = np.random.default_rng(11)
    daily = _daily(index, rng.normal(0.0004, 0.015, len(index)))
    daily.iloc[0, :5] = np.nan
    return daily


def test_validation_and_consistency_need_a_matching_permit() -> None:
    data = from_daily_frame(_synthetic_daily(), qqq_series="proxy", source={"snapshot_id": "s"})

    def half(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
        return pd.Series(float(params["level"]), index=inputs.index)

    for role in ("validation", "consistency_check"):
        with pytest.raises(WindowNotOpenError, match="registered"):
            run_strategy(data, half, RunSpec("qqq_direction", "half", {"level": 0.5}, role), LOADED)
    wrong = WindowPermit("QQQ-Q1", "consistency_check", "QQQ-Q1-w0001", ("half",))
    with pytest.raises(WindowNotOpenError, match="registered"):
        run_strategy(
            data,
            half,
            RunSpec("qqq_direction", "half", {"level": 0.5}, "validation"),
            LOADED,
            permit=wrong,
        )
    with pytest.raises(ValueError, match="plain JSON"):
        run_strategy(
            data,
            half,
            RunSpec("qqq_direction", "half", {"level": object()}, "development"),
            LOADED,
        )
    result = run_strategy(
        data, half, RunSpec("qqq_direction", "half", {"level": 0.5}, "development"), LOADED
    ).to_dict()
    assert result["window"] == {
        "window_id": "qqq_long_history_development",
        "role": "development",
        "start": "1999-03-10",
        "end": "2010-12-31",
    }
    assert set(result["benchmarks"]) == {"matched_constant", "buy_and_hold_qqq", "cash"}
    assert result["cost_bps"] == 4.0
    assert result["checks"]["problems"] == []
    assert len(result["fingerprint"]) == 64


def test_cli_prints_benchmark_summaries_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    daily = _synthetic_daily()

    def fake_from_snapshot(snapshot_dir: Path, *, qqq_series: str) -> Any:
        assert snapshot_dir == tmp_path / "snap"
        return from_daily_frame(
            daily,
            qqq_series="actual" if qqq_series == "actual" else "proxy",
            source={"snapshot_id": "snap", "check_status": "PASS", "check_warnings": []},
        )

    monkeypatch.setattr(cli, "from_snapshot", fake_from_snapshot)
    monkeypatch.setattr(qlh, "resolve_snapshot_dir", lambda root, snapshot: root / snapshot)
    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["benchmarks", "--root", str(tmp_path), "--snapshot", "snap", "--qqq-series", "actual"],
    )
    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[0] == "snapshot snap check PASS warnings -; qqq actual"
    assert lines[1].startswith(
        "window qqq_long_history_development (development) 1999-03-10..2010-12-31"
    )
    assert [line.split()[0] for line in lines[3:]] == [
        "buy_and_hold_qqq",
        "cash",
        "fixed_2x",
        "fixed_3x",
    ]
    assert len(lines) == 7
    bad = runner.invoke(cli.app, ["benchmarks", "--qqq-series", "other"])
    assert bad.exit_code == 1
    assert "must be proxy or actual" in bad.output
    assert "benchmarks" in runner.invoke(cli.app, ["--help"]).output
