"""Acceptance tests for the option index same-drawdown table (TRADING-2567-41).

Written before the code under owner_decision:TRADING-2567:2026-11-22:option_index_first_look.
Every series is synthetic: no test reads a snapshot, the registry is the only real file read.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_data import option_indices as oi
from ai_trading_system.research_probes import leverage_table as lt
from ai_trading_system.research_probes import option_index_table as ot
from ai_trading_system.yaml_loader import safe_load_yaml_path

REGISTRY = PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
SMALL = (0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0)
GOOD_FRIDAY_2022 = pd.Timestamp("2022-04-15")
SKIPPED_NQCLRI = pd.Timestamp("2010-03-15")
SNAPSHOT = "oix-0123456789ab"


# --- a synthetic world -----------------------------------------------------------------------


def _calendar() -> pd.DatetimeIndex:
    days = pd.bdate_range("2008-12-01", "2026-10-12")
    closed = {(1, 1), (7, 4), (12, 25)}
    return pd.DatetimeIndex(
        [d for d in days if (d.month, d.day) not in closed and d != GOOD_FRIDAY_2022]
    )


def _daily() -> pd.DataFrame:
    calendar = _calendar()
    rng = np.random.default_rng(20261122)
    hold = rng.normal(0.0006, 0.011, len(calendar))
    for start, end, drift in (
        ("2010-05-01", "2010-07-01", -0.004),
        ("2018-10-01", "2018-12-24", -0.0045),
        ("2020-02-20", "2020-03-20", -0.02),
        ("2022-01-03", "2022-12-30", -0.0016),
    ):
        hold[(calendar >= pd.Timestamp(start)) & (calendar <= pd.Timestamp(end))] += drift
    hold[0] = np.nan
    cash = np.full(len(calendar), 0.00008)
    cash[0] = np.nan
    return pd.DataFrame({"ndx_tr_return": hold, "cash_return": cash}, index=calendar)


def _levels(daily: pd.DataFrame, first: str, rule: Callable[[np.ndarray], np.ndarray]) -> pd.Series:
    days = daily.index[daily.index >= pd.Timestamp(first)]
    returns = rule(daily["ndx_tr_return"].loc[days].to_numpy(dtype=float, copy=True))
    returns[0] = 0.0
    values = 100.0 * np.cumprod(1.0 + returns)
    series = pd.Series(values, index=days)
    # The snapshot runs on past the QQQ snapshot's last close; the table must cut it there.
    extra = pd.bdate_range("2026-10-13", "2026-11-20")
    return pd.concat([series, pd.Series(series.iloc[-1] * 1.5, index=extra)])


RULES: dict[str, Callable[[np.ndarray], np.ndarray]] = {
    "BXN": lambda h: 0.75 * h + 0.00005,
    "NQTRI": lambda h: np.maximum(h, -0.015) - 0.0001,
    "NQCLRI": lambda h: np.clip(h, -0.01, 0.009),
}


def make_inputs(rules: dict[str, Callable[[np.ndarray], np.ndarray]] | None = None) -> ot.Inputs:
    daily = _daily()
    rules = {**RULES, **(rules or {})}
    levels = {
        "BXN": _levels(daily, "2009-09-18", rules["BXN"]),
        "NQTRI": _levels(daily, "2009-06-19", rules["NQTRI"]),
        "NQCLRI": _levels(daily, "2009-06-19", rules["NQCLRI"]),
    }
    # NQCLRI lacks one QQQ day, NQTRI has one day the QQQ calendar lacks.
    levels["NQCLRI"] = levels["NQCLRI"].drop(SKIPPED_NQCLRI)
    nqtri = levels["NQTRI"]
    levels["NQTRI"] = pd.concat(
        [nqtri, pd.Series([nqtri.loc[:GOOD_FRIDAY_2022].iloc[-1]], index=[GOOD_FRIDAY_2022])]
    ).sort_index()
    windows = ot.registered_windows(safe_load_yaml_path(REGISTRY))
    return ot.Inputs(levels, daily, windows, {"option_snapshot": SNAPSHOT})


@pytest.fixture(scope="module")
def inputs() -> ot.Inputs:
    return make_inputs()


@pytest.fixture(scope="module")
def run(inputs: ot.Inputs) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    reads: list[dict[str, Any]] = []
    result = ot.run_table(inputs, grid=SMALL, on_read=lambda r: reads.append(dict(r)))
    return result, reads


def _floats(value: Any) -> list[float]:
    if isinstance(value, dict):
        return [f for item in value.values() for f in _floats(item)]
    if isinstance(value, list | tuple):
        return [f for item in value for f in _floats(item)]
    return [value] if isinstance(value, float) else []


# --- rolls and the option-leg cost ---------------------------------------------------------


def test_third_fridays_and_roll_days() -> None:
    assert ot.third_friday(2009, 6) == date(2009, 6, 19)
    assert ot.third_friday(2009, 9) == date(2009, 9, 18)
    assert ot.third_friday(2021, 8) == date(2021, 8, 20)
    assert ot.third_friday(2022, 4) == date(2022, 4, 15)
    assert ot.third_friday(2026, 3) == date(2026, 3, 20)
    assert ot.third_friday(2024, 11) == date(2024, 11, 15)
    calendar = _calendar()
    monthly = ot.roll_dates(calendar, tuple(range(1, 13)))
    april = [d for d in monthly if (d.year, d.month) == (2022, 4)]
    assert april == [pd.Timestamp("2022-04-14")]
    quarterly = ot.roll_dates(calendar, (3, 6, 9, 12))
    assert {d.month for d in quarterly} == {3, 6, 9, 12}
    assert pd.Timestamp("2021-09-17") in quarterly and pd.Timestamp("2021-08-20") not in quarterly
    assert len(quarterly) == len({(d.year, d.month) for d in quarterly})
    # Off the calendar the roll moves back to the latest earlier date of the same month ...
    gaps = calendar.drop([pd.Timestamp("2021-09-17"), pd.Timestamp("2021-09-16")])
    assert pd.Timestamp("2021-09-15") in ot.roll_dates(gaps, (9,))
    # ... and a month whose dates start after its third Friday has no roll.
    late = calendar[calendar >= pd.Timestamp("2021-09-20")]
    assert not [d for d in ot.roll_dates(late, (9,)) if d.month == 9 and d.year == 2021]
    # A calendar that ends before a month's third Friday has not rolled in that month yet.
    assert pd.Timestamp("2026-10-12") not in ot.roll_dates(calendar, tuple(range(1, 13)))
    longer = pd.DatetimeIndex(calendar.append(pd.bdate_range("2026-10-13", "2026-10-16")))
    assert pd.Timestamp("2026-10-16") in ot.roll_dates(longer, (10,))


def test_the_leg_cost_is_charged_on_roll_days_on_net_value_and_by_legs(
    inputs: ot.Inputs,
) -> None:
    same = inputs.levels["NQTRI"].loc[lambda s: s.index != GOOD_FRIDAY_2022]
    aligned = {i: ot.align(i, same, inputs.daily) for i in ot.INDICES}
    raw = aligned["NQTRI"].index_return
    for index_id, legs in (("BXN", 1), ("NQTRI", 1), ("NQCLRI", 2)):
        net = ot.net_returns(aligned[index_id], 10.0)
        roll = aligned[index_id].dates.isin(aligned[index_id].rolls)
        k = legs * 10.0 / 10_000
        expected = (1.0 + raw[roll]) * (1.0 - k) - 1.0
        assert np.allclose(
            net[roll].to_numpy(), expected.to_numpy(), rtol=0, atol=1e-15, equal_nan=True
        )
        assert net[~roll].iloc[1:].equals(raw[~roll].iloc[1:])
        down = roll & (raw.to_numpy() < 0)
        assert down.any()
        # On a falling roll day the cost adds to the loss: k times the day's net value.
        assert np.allclose((raw - net)[down], k * (1.0 + raw[down]), rtol=0, atol=1e-15)
    assert set(aligned["NQTRI"].rolls) < set(aligned["BXN"].rolls)
    assert set(aligned["NQTRI"].rolls) == set(aligned["NQCLRI"].rolls)
    assert {d.month for d in aligned["BXN"].rolls} == set(range(1, 13))


def test_levered_cost_follows_the_day_s_drifted_exposure(inputs: ot.Inputs) -> None:
    aligned = ot.align("NQTRI", inputs.levels["NQTRI"], inputs.daily)
    place = ot.layout(aligned, "Q")
    charged = ot.sim_frame(ot.net_returns(aligned, 20.0), aligned.cash_return)
    plain = ot.sim_frame(ot.net_returns(aligned, 0.0), aligned.cash_return)
    targets = pd.Series(1.0, index=charged.index)
    runs = [
        lt.simulate_levered(
            frame,
            targets,
            start=place.anchor.date(),
            end=place.last.date(),
            leverage=2.0,
            costs=lt.MAIN,
        ).simulation.daily
        for frame in (charged, plain)
    ]
    first_roll = place.dates[place.dates.isin(aligned.rolls)][0]
    exposure = float(runs[0]["exposure"].loc[first_roll])
    assert exposure != 2.0  # drifted since the last month end
    assert exposure == float(runs[1]["exposure"].loc[first_roll])
    r = float(aligned.index_return.loc[first_roll])
    k = 20.0 / 10_000
    difference = float(runs[1]["return"].loc[first_roll]) - float(runs[0]["return"].loc[first_roll])
    assert difference == pytest.approx(exposure * k * (1.0 + r), abs=1e-14)
    assert (runs[0]["turnover"].loc[first_roll], runs[0]["cost"].loc[first_roll]) == (0.0, 0.0)


def test_the_calendar_is_the_intersection_with_hold_and_cash_compounded(
    inputs: ot.Inputs,
) -> None:
    aligned = ot.align("NQCLRI", inputs.levels["NQCLRI"], inputs.daily)
    assert aligned.dates[-1] == pd.Timestamp(ot.QLH_LAST_CLOSE)
    assert SKIPPED_NQCLRI not in aligned.dates and aligned.qlh_only == 1
    after = aligned.dates[aligned.dates > pd.Timestamp("2010-03-12")][0]
    assert after == pd.Timestamp("2010-03-16")
    daily = inputs.daily
    two = (daily.index >= SKIPPED_NQCLRI) & (daily.index <= after)
    both = float(np.prod(1.0 + daily["ndx_tr_return"].to_numpy()[two])) - 1.0
    assert aligned.hold_return.loc[after] == pytest.approx(both, abs=1e-15)
    cash = float(np.prod(1.0 + daily["cash_return"].to_numpy()[two])) - 1.0
    assert aligned.cash_return.loc[after] == pytest.approx(cash, abs=1e-15)
    levels = inputs.levels["NQCLRI"]
    expected = float(levels.loc[after]) / float(levels.loc[pd.Timestamp("2010-03-12")]) - 1.0
    assert aligned.index_return.loc[after] == pytest.approx(expected, abs=1e-15)
    nqtri = ot.align("NQTRI", inputs.levels["NQTRI"], inputs.daily)
    assert GOOD_FRIDAY_2022 not in nqtri.dates and nqtri.index_only == 1


# --- check -----------------------------------------------------------------------------------


def test_the_check_reads_dates_counts_and_the_development_part_only(
    inputs: ot.Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    ends: list[date | None] = []
    original = lt.simulate_levered

    def spy(*args: Any, **kwargs: Any) -> Any:
        ends.append(kwargs["end"])
        return original(*args, **kwargs)

    monkeypatch.setattr(lt, "simulate_levered", spy)
    check = ot.check_table(inputs)
    assert check["ok"] is True
    assert ends and all(end is not None and end <= date(2010, 12, 31) for end in ends)
    assert _floats({k: v for k, v in check.items() if k != "rule"}) == []
    for index_id in ot.INDICES:
        row = check["indices"][index_id]
        assert row["self_check_p"]["ok"] is True and row["self_check_p"]["span"] == "P"
        assert _floats(row) == []
        spans = row["spans"]
        assert list(spans) == list(ot.SPANS)
        p_first = "2009-09-21" if index_id == "BXN" else "2009-06-22"
        assert (spans["P"]["first_return"], spans["P"]["last"]) == (p_first, "2010-12-31")
        assert (spans["Q"]["anchor"], spans["Q"]["first_return"]) == ("2010-12-31", "2011-01-03")
        assert (spans["Q1"]["last"], spans["Q2"]["first_return"]) == ("2018-12-31", "2019-01-02")
        assert (spans["LIVE"]["anchor"], spans["LIVE"]["first_return"]) == (
            "2021-08-20",
            "2021-08-23",
        )
        assert spans["Q"]["last"] == spans["Q2"]["last"] == "2026-10-12"
        assert all(s["start_ok"] and s["end_ok"] for s in spans.values())
        assert spans["Q1"]["rolls"] == (12 if index_id == "BXN" else 4) * 8
    assert check["indices"]["NQCLRI"]["qlh_only_days"] == 1
    assert check["indices"]["NQTRI"]["index_only_days"] == 1
    assert check["windows"][ot.READ_WINDOW]["assets"] == list(ot.READ_ASSETS)
    json.dumps(check, allow_nan=False)


@pytest.mark.parametrize(
    ("index_id", "day", "span", "key"),
    [
        ("NQTRI", "2018-12-31", "Q1", "end_ok"),
        ("BXN", "2011-01-03", "Q", "start_ok"),
        ("NQCLRI", "2019-01-02", "Q2", "start_ok"),
        ("NQTRI", "2026-10-12", "Q", "end_ok"),
    ],
)
def test_a_missing_span_boundary_day_fails_the_check(
    index_id: str, day: str, span: str, key: str
) -> None:
    broken = make_inputs()
    levels = dict(broken.levels)
    levels[index_id] = levels[index_id].drop(pd.Timestamp(day))
    check = ot.check_table(broken._replace(levels=levels))
    assert check["ok"] is False
    assert check["indices"][index_id]["spans"][span][key] is False


def test_the_registered_windows_match_the_registry() -> None:
    registry = safe_load_yaml_path(REGISTRY)
    windows = ot.registered_windows(registry)
    assert windows[ot.READ_WINDOW]["assets"] == list(ot.READ_ASSETS)
    assert windows[ot.DEV_WINDOW]["assets"] == list(ot.DEV_ASSETS)
    assert set(ot.OPTION_ASSETS) <= set(ot.READ_ASSETS) and "NDX_TR_RETURN" in ot.READ_ASSETS
    cases: list[Callable[[dict[str, Any]], None]] = [
        lambda r: r["windows"][ot.READ_WINDOW]["assets"].remove("NDX_OPTION_INDEX_NQCLRI"),
        lambda r: r["windows"][ot.DEV_WINDOW]["assets"].remove("NDX_OPTION_INDEX_BXN"),
        lambda r: r["windows"][ot.READ_WINDOW]["caveats"].remove(ot.READ_CAVEAT),
        lambda r: r["windows"][ot.READ_WINDOW].update(read_policy="unlimited"),
        lambda r: r["windows"][ot.READ_WINDOW].update(start="2010-01-04"),
        lambda r: r["windows"][ot.DEV_WINDOW].update(end="2011-12-30"),
    ]
    for change in cases:
        copy = json.loads(json.dumps(registry, default=str))
        change(copy)
        with pytest.raises(ValueError):
            ot.registered_windows(copy)


# --- self-checks -----------------------------------------------------------------------------


@pytest.mark.parametrize("span", ["P", "Q"])
def test_the_self_checks_pass_on_a_correct_table(inputs: ot.Inputs, span: str) -> None:
    for index_id in ot.INDICES:
        aligned = ot.align(index_id, inputs.levels[index_id], inputs.daily)
        found = ot.self_check(aligned, inputs.daily, span)
        assert found == {"span": span, "reproduce": True, "cost": True, "hold": True, "ok": True}


def _wrong_costs(kind: str) -> Callable[[ot.Aligned, float], pd.Series]:
    def net(aligned: ot.Aligned, bps: float) -> pd.Series:
        k = ot.LEGS[aligned.index_id] * bps / 10_000
        r = aligned.index_return
        roll = aligned.dates.isin(aligned.rolls)
        if kind == "times_return":  # K1: the cost scaled by the day's return
            return pd.Series(np.where(roll, r * (1.0 - k), r), index=aligned.dates)
        if kind == "every_day":
            return (1.0 + r) * (1.0 - k) - 1.0
        if kind == "no_legs":
            return pd.Series(np.where(roll, (1.0 + r) * (1.0 - bps / 10_000) - 1.0, r), r.index)
        return r

    return net


@pytest.mark.parametrize(
    ("kind", "index_id"),
    [("times_return", "NQTRI"), ("every_day", "BXN"), ("no_legs", "NQCLRI"), ("none", "NQTRI")],
)
def test_the_cost_self_check_catches_a_wrong_cost(
    inputs: ot.Inputs, monkeypatch: pytest.MonkeyPatch, kind: str, index_id: str
) -> None:
    monkeypatch.setattr(ot, "net_returns", _wrong_costs(kind))
    aligned = ot.align(index_id, inputs.levels[index_id], inputs.daily)
    found = ot.self_check(aligned, inputs.daily, "P")
    assert found["cost"] is False and found["ok"] is False


# --- run -------------------------------------------------------------------------------------


def test_run_records_one_read_and_completes(run: tuple[dict[str, Any], list[Any]]) -> None:
    result, reads = run
    assert result["complete"] is True and result["stop"] is None
    assert result["check"]["ok"] is True
    assert len(reads) == 1 and reads[0] == result["window_read"]
    assert reads[0]["window_id"] == ot.READ_WINDOW
    assert reads[0]["owner_decision"] == ot.OWNER_DECISION
    assert reads[0]["option_snapshot"] == SNAPSHOT
    assert all(item["ok"] and item["span"] == "Q" for item in result["self_check"].values())
    assert set(result["tables"]) == set(ot.INDICES)
    for index_id in ot.INDICES:
        assert list(result["tables"][index_id]) == list(ot.SPANS)
    json.dumps(result, allow_nan=False)


def test_l_star_is_the_largest_grid_leverage_not_deeper_than_hold(
    inputs: ot.Inputs, run: tuple[dict[str, Any], list[Any]]
) -> None:
    """Recomputed here from the levels, the third Fridays and simulate_levered alone."""

    result, _ = run
    daily = inputs.daily
    levels = inputs.levels["NQCLRI"]
    anchor, last = pd.Timestamp("2010-12-31"), pd.Timestamp("2018-12-31")
    dates = daily.index[(daily.index >= pd.Timestamp("2010-11-30")) & (daily.index <= last)]
    r = (levels.loc[dates] / levels.loc[dates].shift(1) - 1.0).to_numpy()
    rolls = {
        max(d for d in dates if d.year == y and d.month == m and d.date() <= ot.third_friday(y, m))
        for y in range(2010, 2019)
        for m in (3, 6, 9, 12)
        if (y, m) > (2010, 11)
    }
    k = 2 * 2.0 / 10_000
    net = np.where(dates.isin(list(rolls)), (1.0 + r) * (1.0 - k) - 1.0, r)
    frame = pd.DataFrame({"qqq": net, "cash": daily["cash_return"].loc[dates]}, index=dates)
    hold = daily["ndx_tr_return"].loc[(daily.index > anchor) & (daily.index <= last)]
    hold_dd = ot.max_drawdown(hold.to_numpy())
    allowed = []
    for leverage in SMALL:
        returns = lt.simulate_levered(
            frame,
            pd.Series(1.0, index=dates),
            start=anchor.date(),
            end=last.date(),
            leverage=leverage,
            costs=lt.MAIN,
        ).simulation.daily["return"]
        allowed.append(ot.max_drawdown(returns.to_numpy()) >= hold_dd)
    expected = max(v for v, ok in zip(SMALL, allowed, strict=True) if ok)
    r2 = result["tables"]["NQCLRI"]["Q1"]["r2"]
    assert r2["leverage"] == expected
    assert r2["flag"] == ("top" if allowed[-1] else "ok")
    assert r2["max_drawdown"] >= hold_dd
    hold_row = result["tables"]["NQCLRI"]["Q1"]["hold"]
    assert hold_row["max_drawdown"] == pytest.approx(hold_dd, abs=1e-12)
    assert r2["d2_main"] == pytest.approx(r2["cagr"] - hold_row["cagr"], abs=1e-12)
    # The robust cell keeps this L* and charges 10 bps a leg with the robust costs.
    wide = 2 * 10.0 / 10_000
    robust_frame = frame.assign(
        qqq=np.where(dates.isin(list(rolls)), (1.0 + r) * (1.0 - wide) - 1.0, r)
    )
    robust = lt.simulate_levered(
        robust_frame,
        pd.Series(1.0, index=dates),
        start=anchor.date(),
        end=last.date(),
        leverage=expected,
        costs=lt.ROBUST,
    ).simulation.daily["return"]
    span_years = (last - anchor).days / 365.25
    robust_cagr = float(np.prod(1.0 + robust.to_numpy())) ** (1.0 / span_years) - 1.0
    assert r2["d2_robust"] == pytest.approx(robust_cagr - hold_row["cagr"], abs=1e-12)
    assert r2["robust_drawdown"] == pytest.approx(ot.max_drawdown(robust.to_numpy()), abs=1e-12)


def test_the_grid_runs_from_0_01_to_5_00() -> None:
    assert ot.L_GRID[0] == 0.01 and ot.L_GRID[-1] == 5.0 and len(ot.L_GRID) == 500
    assert all(round(b - a, 2) == 0.01 for a, b in zip(ot.L_GRID, ot.L_GRID[1:], strict=False))


def test_robust_uses_the_same_l_star_and_prints_its_drawdown(
    run: tuple[dict[str, Any], list[Any]],
) -> None:
    result, _ = run
    for index_id in ot.INDICES:
        for span in ot.SPANS:
            r2 = result["tables"][index_id][span]["r2"]
            hold = result["tables"][index_id][span]["hold"]
            assert r2["d2_robust"] < r2["d2_main"]
            assert r2["robust_deeper_by"] == pytest.approx(
                r2["robust_drawdown"] - hold["max_drawdown"], abs=1e-15
            )
            assert r2["vol_leverage"] in SMALL and r2["delever"] == (r2["leverage"] < 1.0)


def test_cost_sensitivity_redoes_l_star_for_each_cost(
    run: tuple[dict[str, Any], list[Any]],
) -> None:
    result, _ = run
    for index_id in ot.INDICES:
        for span in ot.FULL_SPANS:
            table = result["tables"][index_id][span]
            rows = table["sensitivity"]
            assert [row["bps"] for row in rows] == [0.0, 2.0, 5.0, 10.0, 20.0]
            main = next(row for row in rows if row["bps"] == 2.0)
            assert (main["leverage"], main["d2_main"]) == (
                table["r2"]["leverage"],
                table["r2"]["d2_main"],
            )
            levers = [row["leverage"] for row in rows]
            assert levers == sorted(levers, reverse=True)
            for low, high in zip(rows, rows[1:], strict=False):
                if low["leverage"] == high["leverage"]:
                    assert high["d2_main"] < low["d2_main"]
        assert "sensitivity" not in result["tables"][index_id]["LIVE"]


def test_beta_is_the_least_squares_slope_and_the_comparator_is_gross(
    inputs: ot.Inputs, run: tuple[dict[str, Any], list[Any]]
) -> None:
    x = np.linspace(-0.03, 0.03, 101)
    assert ot.slope(0.7 * x + 0.001, x) == pytest.approx(0.7, abs=1e-12)
    result, _ = run
    daily = inputs.daily
    anchor, last = pd.Timestamp("2010-12-31"), pd.Timestamp("2026-10-12")
    dates = daily.index[daily.index >= pd.Timestamp("2010-11-30")]
    frame = pd.DataFrame(
        {"qqq": daily["ndx_tr_return"].loc[dates], "cash": daily["cash_return"].loc[dates]}
    )
    beta = result["tables"]["BXN"]["Q"]["beta"]["beta"]
    fixed = lt.simulate_levered(
        frame,
        pd.Series(1.0, index=dates),
        start=anchor.date(),
        end=last.date(),
        leverage=beta,
        costs=lt.Costs(0.0, 0.0),
    ).simulation.daily["return"]
    span_years = (last - anchor).days / 365.25
    expected = float(np.prod(1.0 + fixed.to_numpy())) ** (1.0 / span_years) - 1.0
    assert result["tables"]["BXN"]["Q"]["beta"]["cagr"] == pytest.approx(expected, abs=1e-12)
    for span in ot.FULL_SPANS:
        table = result["tables"]["BXN"][span]
        assert table["beta"]["beta"] == pytest.approx(0.75, abs=1e-9)
        assert table["raw"]["beta"] == pytest.approx(0.75, abs=1e-9)
        assert table["beta"]["index_minus_fixed"] == pytest.approx(
            table["raw"]["cagr"] - table["beta"]["cagr"], abs=1e-15
        )


def test_bear_and_year_rows_cover_each_span(run: tuple[dict[str, Any], list[Any]]) -> None:
    result, _ = run
    for index_id in ot.INDICES:
        q = result["tables"][index_id]["Q"]
        assert [row["year"] for row in q["years"]] == list(range(2011, 2027))
        assert [row["year"] for row in result["tables"][index_id]["Q1"]["years"]] == list(
            range(2011, 2019)
        )
        assert [row["year"] for row in result["tables"][index_id]["P"]["years"]] == [2009, 2010]
        assert q["bear"]["segments"], "the synthetic 2022 fall is a bear segment"
        assert 0 < q["bear"]["inside_days"] < q["return_days"]
        assert set(q["bear"]["lines"]) == {"unlevered", "levered"}
        assert all(s["kind"] != "long_bull" for s in q["bear"]["segments"])


def test_the_live_row_prints_the_seam_day_residual(
    inputs: ot.Inputs, run: tuple[dict[str, Any], list[Any]]
) -> None:
    result, _ = run
    seam, before = pd.Timestamp("2021-08-23"), pd.Timestamp("2021-08-20")
    for index_id in ("NQTRI", "NQCLRI"):
        levels = inputs.levels[index_id]
        own = levels.loc[seam] / levels.loc[before] - 1.0
        hold = inputs.daily["ndx_tr_return"].loc[seam]
        live = result["tables"][index_id]["LIVE"]
        assert live["seam_residual"] == pytest.approx(own - hold, abs=1e-15)
        assert (live["first_return"], live["last"]) == ("2021-08-23", "2026-10-12")
        assert {"raw", "hold", "r2"} <= set(live)
    assert "seam_residual" not in result["tables"]["BXN"]["LIVE"]


def test_the_summary_and_verdicts_follow_the_rule(run: tuple[dict[str, Any], list[Any]]) -> None:
    result, _ = run
    assert [row["index"] for row in result["summary"]] == list(ot.INDICES)
    for row in result["summary"]:
        assert list(row["spans"]) == list(ot.JUDGED)
        cells = {s: result["tables"][row["index"]][s]["r2"] for s in ot.JUDGED}
        assert row["verdict"] == ot.verdict(cells) == result["verdicts"][row["index"]]
    assert result["reading"] == ot.reading(result["verdicts"])
    text = ot.render_markdown(result)
    assert "## 总表" in text and all(i in text for i in ot.INDICES)
    assert result["reading"]["text"] in text


def _row(flag: str = "ok", main: float = 0.02, robust: float = 0.001) -> dict[str, Any]:
    return {"flag": flag, "d2_main": main, "d2_robust": robust}


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ({"Q": _row(), "Q1": _row(), "Q2": _row()}, "holds"),
        ({"Q": _row(main=0.01), "Q1": _row(), "Q2": _row(robust=0.0)}, "holds"),
        ({"Q": _row(), "Q1": _row(), "Q2": _row(main=0.0099)}, "not_holds"),
        ({"Q": _row(), "Q1": _row(robust=-0.0001), "Q2": _row()}, "not_holds"),
        ({"Q": _row(), "Q1": _row(flag="top", main=-0.5), "Q2": _row()}, "not_judged"),
        ({"Q": _row(), "Q1": _row(flag="top"), "Q2": _row(main=-0.01)}, "not_holds"),
        ({"Q": _row(flag="bottom"), "Q1": _row(), "Q2": _row()}, "not_judged"),
        ({"P": _row(main=-1.0), "Q": _row(), "Q1": _row(), "Q2": _row()}, "holds"),
    ],
)
def test_one_index_holds_only_when_all_three_spans_pass_both_lines(
    rows: dict[str, Any], expected: str
) -> None:
    assert ot.verdict(rows) == expected


@pytest.mark.parametrize(
    ("verdicts", "case"),
    [
        ({"BXN": "not_holds", "NQTRI": "not_holds", "NQCLRI": "not_holds"}, "none_holds"),
        ({"BXN": "not_holds", "NQTRI": "not_judged", "NQCLRI": "not_holds"}, "not_judged"),
        ({"BXN": "not_holds", "NQTRI": "not_judged", "NQCLRI": "holds"}, "one_holds"),
    ],
)
def test_the_reading_follows_the_three_verdicts(verdicts: dict[str, str], case: str) -> None:
    found = ot.reading(verdicts)
    assert found["case"] == case and found["text"] == ot.READINGS[case]
    assert found["not_judged"] == [i for i in ot.INDICES if verdicts[i] == "not_judged"]


def test_top_and_delever_are_flagged_and_not_judged_reads_as_closed() -> None:
    extremes = make_inputs({"BXN": lambda h: 1.4 * h, "NQTRI": lambda h: 0.02 * h + 0.0003})
    result = ot.run_table(extremes, grid=(0.5, 0.75, 1.0, 2.0, 5.0))
    assert result["complete"] is True
    for span in ot.JUDGED:
        bxn = result["tables"]["BXN"][span]["r2"]
        assert bxn["delever"] is True and bxn["leverage"] < 1.0 and bxn["flag"] == "ok"
        nqtri = result["tables"]["NQTRI"][span]["r2"]
        assert (nqtri["flag"], nqtri["leverage"]) == ("top", 5.0)
    assert result["verdicts"]["NQTRI"] in {"not_judged", "not_holds"}
    if "holds" not in result["verdicts"].values():
        assert result["reading"]["case"] in {"not_judged", "none_holds"}
    assert ot.TOP_TEXT in ot.render_markdown(result)


def test_a_bound_problem_stops_before_any_table(
    inputs: ot.Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[float, float]] = []

    def bound(buy_hold: float, line: float) -> float:
        calls.append((buy_hold, line))
        return 0.2

    monkeypatch.setattr(lt, "drawdown_bound", bound)
    reads: list[Any] = []
    result = ot.run_table(inputs, grid=SMALL, on_read=reads.append)
    assert result["complete"] is False and result["stop"] == ot.STOP_BOUND
    assert "tables" not in result and "summary" not in result and "verdicts" not in result
    assert result["bound_problem"]["index"] == "BXN" and result["bound_problem"]["span"] == "P"
    assert len(reads) == 1
    assert calls and all(a < 0 and b < 0 for a, b in calls)


def test_a_failed_self_check_on_q_stops_before_any_table(
    inputs: ot.Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    order: list[str] = []
    original_check, original_table = ot.self_check, ot.span_table

    def check(aligned: ot.Aligned, daily: pd.DataFrame, span: str) -> dict[str, Any]:
        order.append(f"check {span}")
        found = original_check(aligned, daily, span)
        return {**found, "hold": False, "ok": False} if span == "Q" else found

    def table(*args: Any, **kwargs: Any) -> Any:
        order.append("table")
        return original_table(*args, **kwargs)

    monkeypatch.setattr(ot, "self_check", check)
    monkeypatch.setattr(ot, "span_table", table)
    reads: list[Any] = []
    result = ot.run_table(inputs, grid=SMALL, on_read=reads.append)
    assert result["complete"] is False and result["stop"] == ot.STOP_SELF_CHECK
    assert "tables" not in result and len(reads) == 1
    assert order.count("check Q") == 3 and "table" not in order


def test_the_self_checks_on_q_come_before_the_tables(
    inputs: ot.Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    order: list[str] = []
    original_check, original_table = ot.self_check, ot.span_table

    def check(aligned: ot.Aligned, daily: pd.DataFrame, span: str) -> dict[str, Any]:
        order.append(f"check {span}")
        return original_check(aligned, daily, span)

    def table(*args: Any, **kwargs: Any) -> Any:
        order.append("table")
        return original_table(*args, **kwargs)

    monkeypatch.setattr(ot, "self_check", check)
    monkeypatch.setattr(ot, "span_table", table)
    result = ot.run_table(inputs, grid=(1.0, 2.0))
    assert result["complete"] is True
    first_table = order.index("table")
    assert order[:first_table] == ["check P"] * 3 + ["check Q"] * 3


def test_a_failed_check_reads_nothing(inputs: ot.Inputs) -> None:
    levels = dict(inputs.levels)
    levels["BXN"] = levels["BXN"].drop(pd.Timestamp("2011-01-03"))
    reads: list[Any] = []
    result = ot.run_table(inputs._replace(levels=levels), grid=SMALL, on_read=reads.append)
    assert result["complete"] is False and result["stop"] == ot.STOP_CHECK
    assert reads == [] and "window_read" not in result and "self_check" not in result


# --- commands --------------------------------------------------------------------------------


def test_the_commands_refuse_to_overwrite_and_run_reads_once(
    tmp_path: Path, inputs: ot.Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[tuple[Path, Path, str]] = []

    def load(root: Path, qlh_root: Path, snapshot: str) -> ot.Inputs:
        seen.append((root, qlh_root, snapshot))
        return inputs

    original = ot.run_table
    monkeypatch.setattr(ot, "load_inputs", load)
    monkeypatch.setattr(ot, "run_table", lambda i, **k: original(i, **{**k, "grid": (1.0, 2.0)}))
    runner = CliRunner()
    common = ["--root", str(tmp_path), "--qlh-root", str(tmp_path / "qlh")]
    checked = runner.invoke(ot.app, ["check", "--out", str(tmp_path / "c"), *common])
    assert checked.exit_code == 0, checked.output
    check = json.loads((tmp_path / "c" / ot.CHECK_JSON).read_text(encoding="utf-8"))
    assert check["ok"] is True
    again = runner.invoke(ot.app, ["check", "--out", str(tmp_path / "c"), *common])
    assert again.exit_code == 1
    first = runner.invoke(
        ot.app, ["run", "--out", str(tmp_path / "r1"), *common, "--snapshot", SNAPSHOT]
    )
    assert first.exit_code == 0, first.output
    assert seen[-1] == (tmp_path, tmp_path / "qlh", SNAPSHOT)
    result = json.loads((tmp_path / "r1" / ot.RESULT_JSON).read_text(encoding="utf-8"))
    assert result["complete"] is True
    assert (tmp_path / "r1" / ot.RESULT_MD).exists()
    record = ot.read_record_path(tmp_path)
    assert record.parent.parent == Path(oi.snapshot_root(tmp_path))
    stored = json.loads(record.read_text(encoding="utf-8"))
    assert stored["window_id"] == ot.READ_WINDOW and stored["run_id"] == ot.RUN_ID
    second = runner.invoke(ot.app, ["run", "--out", str(tmp_path / "r2"), *common])
    assert second.exit_code == 1
    assert not (tmp_path / "r2" / ot.RESULT_JSON).exists()
    assert len(seen) == 2  # the second run is refused before loading anything


def test_a_stopped_run_keeps_its_read_record_and_exits_non_zero(
    tmp_path: Path, inputs: ot.Inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ot, "load_inputs", lambda root, qlh_root, snapshot: inputs)
    monkeypatch.setattr(lt, "drawdown_bound", lambda a, b: 0.2)
    original = ot.run_table
    monkeypatch.setattr(ot, "run_table", lambda i, **k: original(i, **{**k, "grid": (1.0, 2.0)}))
    out = tmp_path / "r"
    found = CliRunner().invoke(ot.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert found.exit_code == 1 and ot.STOP_BOUND in found.output
    assert ot.read_record_path(tmp_path).exists()
    assert json.loads((out / ot.RESULT_JSON).read_text())["complete"] is False
    assert not (out / ot.RESULT_MD).exists()


def test_the_module_reads_only_qqq_its_index_and_cash() -> None:
    source = Path(ot.__file__).read_text(encoding="utf-8")
    for word in (
        "us_market",
        "ff_factors",
        "lt.load_data",
        "pt.load_data",
        "policy_table",
        "ndx_price_1986_1999",
        "etf_reference_return",
    ):
        assert word not in source
    assert ot.QLH_SNAPSHOT_ID == "qlh-20261013-0b0df53cb1a2"
    assert ot.OWNER_DECISION == "owner_decision:TRADING-2567:2026-11-22:option_index_first_look"
    assert (ot.LEGS, ot.LEG_BPS_MAIN, ot.LEG_BPS_ROBUST) == (
        {"BXN": 1, "NQTRI": 1, "NQCLRI": 2},
        2.0,
        10.0,
    )
    assert (ot.HOLDS_MAIN, ot.HOLDS_ROBUST) == (0.01, 0.0)
    assert (lt.MAIN, lt.ROBUST) == (lt.Costs(0.01, 4.0), lt.Costs(0.02, 25.0))
    assert math.isclose(ot.SELF_CHECK_TOLERANCE, 1e-9)
