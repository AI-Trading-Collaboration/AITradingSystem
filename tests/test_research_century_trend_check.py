"""TRADING-2565-41: the 1871-1928 trend-overlay check on Shiller's monthly data (synthetic data).

The probe divides the total-return level by the slow-bear probe's rule, holds each monthly trend
equivalent a month late against the fixed mix at its average exposure, and reads only rules fixed
before any run. Every number here comes from synthetic frames; nothing reads a snapshot.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_bench import segments as seg
from ai_trading_system.research_probes import century_trend_check as ct

MONTHS = pd.date_range("1871-01-01", "2026-10-01", freq="MS")
DAYS = pd.bdate_range("1926-07-01", "2012-12-31")


def _frames(seed: int = 5, *, dividend_yield: float = 0.05) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    price = 5 * np.exp(np.cumsum(rng.normal(0.004, 0.04, len(MONTHS))))
    rate = np.clip(4 + np.cumsum(rng.normal(0.0, 0.05, len(MONTHS))), 2.5, 7.5)
    return {
        ct.PRICE_SOURCE: pd.DataFrame({"value": price}, index=MONTHS),
        ct.DIVIDEND_SOURCE: pd.DataFrame({"value": price * dividend_yield}, index=MONTHS),
        ct.RATE_SOURCE: pd.DataFrame({"value": rate}, index=MONTHS),
        "ff_factors_daily": pd.DataFrame(
            {
                "Mkt-RF": rng.normal(0.0003, 0.011, len(DAYS)),
                "RF": np.full(len(DAYS), 0.0001),
            },
            index=DAYS,
        ),
    }


def _run(frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    return ct.run_probe(frames, snapshot_id="sig-test", provenance={"commit": "test"})


def _returns(path: list[float], start: str = "1872-03") -> pd.Series:
    return pd.Series(path, index=pd.date_range(f"{start}-01", periods=len(path), freq="MS"))


def _shaped() -> pd.Series:
    """Rise 6 months, fall 30% over 9, recover, rise 4 years, fall 30% over 2, recover."""

    slow = 0.7 ** (1 / 9) - 1
    fast = 0.7 ** (1 / 2) - 1
    path = [0.01] * 6 + [slow] * 9 + [0.04] * 12 + [0.01] * 48 + [fast] * 2 + [0.04] * 12
    return _returns(path)


@pytest.fixture(scope="module")
def result() -> dict[str, Any]:
    return _run(_frames())


def test_the_rules_are_fixed_before_the_run() -> None:
    assert ct.LOOKBACKS == {
        "slow_only_m": (12,),
        "fast_slow_m": (1, 12),
        "trend4_m": (1, 3, 6, 12),
    }
    assert (ct.LAG, ct.CASH, ct.FIRST_MONTH, ct.LAST_MONTH, ct.EVAL_FIRST) == (
        1,
        "gs10",
        "1871-01",
        "1928-12",
        "1872-03",
    )
    assert (ct.HOLD_NUMERATOR, ct.HOLD_DENOMINATOR, ct.MIN_HOLDING_CONFIGS) == (2, 3, 2)
    assert (ct.MIN_SLOW_BEARS, ct.COST_BPS, ct.TAIL_SHARE) == (4, 25.0, 0.05)
    assert (ct.RATE_LOW, ct.RATE_HIGH, ct.WINDOW_MONTHS) == (2.0, 8.0, 696)
    assert ct.DAILY_POSITIVE == {"slow_only_m": 8, "fast_slow_m": 10, "trend4_m": 9}
    assert (ct.DAILY_SLOW_BEARS, ct.FIDELITY_SEGMENTS, ct.DECISIVE_VARIANT) == (
        10,
        2,
        "average_lag1",
    )
    assert (ct.FIDELITY_WINDOW.start.isoformat(), ct.FIDELITY_WINDOW.end.isoformat()) == (
        "1929-01-02",
        "2010-12-31",
    )
    assert (seg.DROP, seg.SLOW_MONTHS, seg.LONG_BULL_MONTHS) == (0.20, 6, 36)
    assert ct.SNAPSHOT_ID == "sig-ed9e988ffe7c"
    assert ct.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-21:period3_direction"
    assert (ct.PRICE_SOURCE, ct.DIVIDEND_SOURCE, ct.RATE_SOURCE) == (
        "shiller_price",
        "shiller_dividend",
        "shiller_gs10",
    )


# (a) the slow-bear probe's segment rule on a monthly level


def test_the_segments_follow_the_slow_bear_probe_rule() -> None:
    found = ct.find_segments(_shaped())
    assert [(item.kind, item.start, item.end, item.months) for item in found] == [
        ("slow_bear", "1872-09", "1873-05", 9),
        ("long_bull", "1873-06", "1878-05", 60),
        ("fast_drop", "1878-06", "1878-07", 2),
    ]
    slow = found[0]
    assert (slow.id, slow.peak, slow.trough) == ("slow_bear_187208", "1872-08", "1873-05")
    assert slow.change == pytest.approx(-0.3, abs=1e-12)
    assert found[2].change == pytest.approx(-0.3, abs=1e-12)
    level = (1 + _shaped()).cumprod()
    level.index = level.index.to_period("M")
    drops = seg.drop_segments(level)
    assert found == sorted(
        [*drops, *seg.long_bull_segments(level, drops)], key=lambda item: item.start
    )


def test_a_fall_lasting_six_months_is_a_slow_bear_and_five_a_fast_drop() -> None:
    for months, kind in ((6, "slow_bear"), (5, "fast_drop")):
        step = 0.7 ** (1 / months) - 1
        found = ct.find_segments(_returns([0.01] * 3 + [step] * months + [0.05] * 12))
        assert [(item.kind, item.months) for item in found] == [(kind, months)]


# (b) a signal out of the market in every slow bear beats the fixed mix there


def test_an_ideal_signal_beats_the_fixed_mix_in_every_slow_bear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rng = np.random.default_rng(3)
    path = list(rng.normal(0.008, 0.02, 30))
    for _ in range(3):
        path += [0.7 ** (1 / 8) - 1] * 8 + list(rng.normal(0.02, 0.01, 40))
    market = _returns(path, "1872-01")
    cash = pd.Series(0.002, index=market.index)
    items = [item for item in ct.find_segments(market) if item.kind == "slow_bear"]
    assert len(items) == 3
    months = market.index.to_period("M")
    inside = np.zeros(len(market), dtype=bool)
    for item in items:
        inside |= (months >= pd.Period(item.start, "M")) & (months <= pd.Period(item.end, "M"))
    ideal = pd.Series(np.where(inside, 0.0, 1.0), index=market.index).shift(-2)
    monkeypatch.setattr(ct, "exposure", lambda level, lookbacks: ideal)
    first, last = f"{months[2]}", f"{months[-1]}"
    found = ct.sections(market, cash, market, 1, first, last, items)
    for name in ct.CONFIGS:
        rows = found[name]["rows"]
        assert len(rows) == 3
        for row in rows:
            assert row["difference"] > 0
            assert row["average_exposure"] == 0.0
            assert row["strategy_return"] == pytest.approx(1.002 ** row["months"] - 1)


# (c) with no cash, the fixed mix earns its share of each month (rebalanced monthly)


def test_with_no_cash_the_fixed_mix_earns_its_share_of_each_month() -> None:
    market = _shaped()
    zero = pd.Series(0.0, index=market.index)
    signal = pd.Series(np.resize([1.0, 0.0, 1.0, 1.0], len(market)), index=market.index)
    held = ct.held(signal, 1)
    first, last = "1872-06", f"{market.index[-1]:%Y-%m}"
    pair = ct.pair_returns(market, zero, held, first, last)
    share = pair.average_exposure
    inside = market.loc[pd.Timestamp("1872-06-01") :]
    assert share == pytest.approx(float(held.loc[inside.index].mean()))
    assert np.allclose(pair.benchmark.to_numpy(), share * inside.to_numpy(), atol=1e-15)
    slow = ct.find_segments(market)[0]
    within = inside.loc[pd.Timestamp(f"{slow.start}-01") : pd.Timestamp(f"{slow.end}-01")]
    assert seg.segment_return(pair.benchmark, slow) == pytest.approx(
        float(np.prod(1 + share * within.to_numpy()) - 1), abs=1e-15
    )
    always = ct.pair_returns(market, zero, pd.Series(1.0, index=market.index), first, last)
    assert seg.segment_return(always.benchmark, slow) == pytest.approx(slow.change, abs=1e-12)


def test_the_strategy_holds_its_exposure_and_the_rest_in_cash() -> None:
    market = _returns([0.05, -0.04, 0.03, 0.02])
    cash = pd.Series([0.001, 0.002, 0.003, 0.004], index=market.index)
    holding = pd.Series([1.0, 0.5, 0.0, 0.75], index=market.index)
    pair = ct.pair_returns(market, cash, holding, "1872-03", "1872-06")
    expected = holding * market + (1 - holding) * cash
    assert np.allclose(pair.strategy.to_numpy(), expected.to_numpy(), atol=1e-15)
    assert pair.average_exposure == pytest.approx(0.5625)
    assert np.allclose(
        pair.benchmark.to_numpy(), (0.5625 * market + 0.4375 * cash).to_numpy(), atol=1e-15
    )
    assert pair.turnover.tolist() == [0.0, 0.5, 0.5, 0.75]
    with pytest.raises(ValueError, match="missing value in 1872-07"):
        ct.pair_returns(market, cash, holding, "1872-03", "1872-07")
    gap = holding.copy()
    gap.iloc[1] = np.nan
    with pytest.raises(ValueError, match="missing value in 1872-04"):
        ct.pair_returns(market, cash, gap, "1872-03", "1872-06")


# (d) a month's signal is held one month later at lag 1 than at lag 0


def test_lag_one_is_lag_zero_a_month_later() -> None:
    signal = pd.Series([0.0, 0.5, 1.0, 0.25, 0.75], index=MONTHS[:5])
    assert ct.held(signal, 0).equals(signal.shift(1))
    assert ct.held(signal, 1).equals(ct.held(signal, 0).shift(1))
    assert ct.held(signal, 1).tolist()[2:] == [0.0, 0.5, 1.0]
    with pytest.raises(ValueError, match="lag must be one of"):
        ct.held(signal, 2)


# (e) rows after 1928-12 are cut before anything reads them


def _cut(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    out = dict(frames)
    for source in (ct.PRICE_SOURCE, ct.DIVIDEND_SOURCE, ct.RATE_SOURCE):
        frame = frames[source]
        out[source] = frame.loc[frame.index <= pd.Timestamp("1928-12-01")]
    out["ff_factors_daily"] = frames["ff_factors_daily"].loc[:"2010-12-31"]
    return out


def test_rows_after_1928_are_never_read(result: dict[str, Any]) -> None:
    frames = _frames()
    spoiled = dict(frames)
    for source, bad in (
        (ct.PRICE_SOURCE, -1.0),
        (ct.DIVIDEND_SOURCE, np.nan),
        (ct.RATE_SOURCE, 50.0),
    ):
        frame = frames[source].copy()
        frame.loc[frame.index >= pd.Timestamp("1929-01-01"), "value"] = bad
        spoiled[source] = frame
    market = frames["ff_factors_daily"].copy()
    market.loc[market.index > pd.Timestamp("2010-12-31"), "Mkt-RF"] = 0.5
    spoiled["ff_factors_daily"] = market
    inputs = ct.shiller_inputs(spoiled)
    assert inputs.equals(ct.shiller_inputs(_cut(frames)))
    assert (str(inputs.index[0]), str(inputs.index[-1]), len(inputs)) == ("1871-01", "1928-12", 696)
    assert ct.data_check(inputs)["ok"]
    assert json.dumps(ct._clean(_run(spoiled))) == json.dumps(ct._clean(result))
    assert json.dumps(ct._clean(_run(_cut(frames)))) == json.dumps(ct._clean(result))


# (f) the report and the result hold tables and summaries, never monthly rows


def test_the_report_has_no_data_rows(result: dict[str, Any]) -> None:
    frames = _frames()
    frames[ct.PRICE_SOURCE].loc[pd.Timestamp("1900-06-01"), "value"] = 1234.5678
    found = _run(frames)
    text = ct.render_markdown(found)
    dumped = json.dumps(ct._clean(found), ensure_ascii=False)
    for sentinel in ("1234.5678", "1234.57", "1234.6"):
        assert sentinel not in text and sentinel not in dumped
    segments = len(found["segments"])

    def lists(value: Any) -> list[int]:
        if isinstance(value, dict):
            return [n for item in value.values() for n in lists(item)]
        if isinstance(value, list):
            return [len(value), *(n for item in value for n in lists(item))]
        return []

    assert max(lists(found)) <= max(segments, 4)
    table_rows = [line for line in text.splitlines() if line.startswith("| ")]
    assert len(table_rows) < 4 * segments + 40
    assert len(text.splitlines()) < 6 * segments + 80


def test_the_total_return_and_cash_follow_the_formulas() -> None:
    inputs = pd.DataFrame(
        {"P": [10.0, 11.0, 12.0], "D": [1.2, 1.2, 2.4], "GS10": [6.0, 3.0, 4.0]},
        index=pd.period_range("1871-01", periods=3, freq="M"),
    )
    total = ct.total_returns(inputs)
    assert total.index.tolist() == [pd.Timestamp("1871-02-01"), pd.Timestamp("1871-03-01")]
    assert total.tolist() == pytest.approx([(11 + 0.1) / 10 - 1, (12 + 0.2) / 11 - 1], abs=1e-15)
    assert ct.cash_returns(inputs, "gs10").tolist() == pytest.approx([0.005, 0.0025], abs=1e-15)
    assert ct.cash_returns(inputs, "zero").tolist() == [0.0, 0.0]
    with pytest.raises(ValueError, match="unknown cash"):
        ct.cash_returns(inputs, "dtb3")


def test_each_config_holds_the_share_of_positive_lookbacks() -> None:
    values = [100.0] * 13
    values[0] = 120.0  # twelve months back: higher
    values[6] = 90.0  # six months back: lower
    values[9] = 101.0  # three months back: higher
    values[11] = 99.0  # one month back: lower
    level = pd.Series(values, index=MONTHS[:13])
    exposure = {name: ct.exposure(level, ct.LOOKBACKS[name]) for name in ct.CONFIGS}
    assert exposure["slow_only_m"].iloc[-1] == 0.0
    assert exposure["fast_slow_m"].iloc[-1] == 0.5
    assert exposure["trend4_m"].iloc[-1] == 0.5
    for name in ct.CONFIGS:
        assert exposure[name].iloc[:12].isna().all()
    flat = ct.exposure(pd.Series([1.0] * 14, index=MONTHS[:14]), (1, 12))
    assert flat.iloc[12:].tolist() == [0.0, 0.0]
    assert ct.exposure(level, (1,)).iloc[1:].tolist() == [
        float(b > a) for a, b in zip(values[:-1], values[1:], strict=True)
    ]


def test_the_signal_is_the_price_not_the_total_return() -> None:
    frames = _frames()
    falling = 5 * 0.995 ** np.arange(len(MONTHS))
    frames[ct.PRICE_SOURCE] = pd.DataFrame({"value": falling}, index=MONTHS)
    frames[ct.DIVIDEND_SOURCE] = pd.DataFrame({"value": falling * 0.12}, index=MONTHS)
    found = _run(frames)
    assert [item["kind"] for item in found["segments"]] == ["long_bull"]
    for name in ct.CONFIGS:
        assert found["sections"]["main"][name]["average_exposure"] == 0.0


def test_the_main_reading_is_evaluated_from_march_1872(result: dict[str, Any]) -> None:
    inputs = ct.shiller_inputs(_frames())
    price = pd.Series(inputs["P"].to_numpy(), index=inputs.index.to_timestamp())
    months = pd.date_range("1872-03-01", "1928-12-01", freq="MS")
    for key, lag in (("main", 1), ("lag0", 0), ("cash_zero", 1)):
        for name in ct.CONFIGS:
            holding = ct.held(ct.exposure(price, ct.LOOKBACKS[name]), lag)
            assert result["sections"][key][name]["average_exposure"] == pytest.approx(
                float(holding.loc[months].mean()), abs=1e-15
            )
    evaluated = ct.total_returns(inputs).loc[months]
    assert [ct.Segment(**item) for item in result["segments"]] == ct.find_segments(evaluated)
    assert all(item["start"] >= "1872-03" for item in result["segments"])


def test_the_three_tables_use_their_own_lag_and_cash(result: dict[str, Any]) -> None:
    inputs = ct.shiller_inputs(_frames())
    market = ct.total_returns(inputs)
    price = pd.Series(inputs["P"].to_numpy(), index=inputs.index.to_timestamp())
    items = [ct.Segment(**item) for item in result["segments"]]
    for key, lag, cash in (("main", 1, "gs10"), ("lag0", 0, "gs10"), ("cash_zero", 1, "zero")):
        expected = ct.sections(
            market, ct.cash_returns(inputs, cash), price, lag, "1872-03", "1928-12", items
        )
        assert json.dumps(ct._clean(expected)) == json.dumps(ct._clean(result["sections"][key]))


def test_segment_rows_compare_the_strategy_with_the_fixed_mix() -> None:
    market = _shaped()
    cash = pd.Series(0.001, index=market.index)
    holding = pd.Series(np.linspace(0.2, 0.9, len(market)), index=market.index)
    first, last = "1872-03", f"{market.index[-1]:%Y-%m}"
    pair = ct.pair_returns(market, cash, holding, first, last)
    items = ct.find_segments(market)
    rows = ct.segment_rows(pair, items)
    for item, row in zip(items, rows, strict=True):
        own = seg.segment_return(pair.strategy, item)
        reference = seg.segment_return(pair.benchmark, item)
        assert row["difference"] == own - reference
        months = holding.index.to_period("M")
        inside = (months >= pd.Period(item.start, "M")) & (months <= pd.Period(item.end, "M"))
        assert row["average_exposure"] == pytest.approx(float(holding.loc[inside].mean()))
        assert (row["id"], row["kind"], row["months"], row["change"]) == (
            item.id,
            item.kind,
            item.months,
            item.change,
        )
    bull = next(row for row in rows if row["kind"] == "long_bull")
    assert bull["strategy_annual"] == pytest.approx((1 + bull["strategy_return"]) ** (12 / 60) - 1)
    assert bull["annual_difference"] == bull["strategy_annual"] - bull["benchmark_annual"]
    assert "annual_difference" not in rows[0]


def _rows(differences: list[float], kind: str = "slow_bear") -> list[dict[str, Any]]:
    return [{"kind": kind, "difference": value} for value in differences]


def test_summaries_count_positive_differences() -> None:
    rows = _rows([0.10, -0.02, 0.05]) + _rows([0.01], "fast_drop")
    rows.append({"kind": "long_bull", "difference": 0.3, "annual_difference": -0.02})
    rows.append({"kind": "long_bull", "difference": 0.1, "annual_difference": -0.01})
    rows.append({"kind": "long_bull", "difference": 0.2, "annual_difference": -0.06})
    found = ct.summary(rows)
    assert found["slow_bear"] == {
        "count": 3,
        "median_difference": 0.05,
        "positive": 2,
        "positive_share": pytest.approx(2 / 3),
        "min_difference": -0.02,
    }
    assert found["fast_drop"]["count"] == 1 and found["fast_drop"]["positive"] == 1
    assert found["long_bull"] == {"count": 3, "median_annual_difference": -0.02}
    empty = ct.summary([])
    assert empty["slow_bear"]["median_difference"] is None
    assert empty["slow_bear"]["positive_share"] is None
    assert ct.summary(_rows([0.0]))["slow_bear"]["positive"] == 0


@pytest.mark.parametrize(
    ("differences", "holds"),
    [
        ([0.1, 0.2, -0.1], True),  # exactly 2/3 positive
        ([0.1, -0.2, -0.1], False),
        ([0.1, 0.1, 0.1, -0.1, -0.1, -0.1], False),  # half
        ([0.1, 0.1, 0.0, 0.0, 0.1, 0.1], True),  # 4 of 6
        ([0.1, 0.1, 0.1, 0.1, 0.1, -0.1, -0.1, -0.1, -0.1], False),  # 5 of 9
        ([0.0, 0.1, 0.1], True),  # zero is not positive, median > 0
        ([], False),
    ],
)
def test_a_config_holds_on_two_thirds_positive_and_a_positive_median(
    differences: list[float], holds: bool
) -> None:
    assert ct.config_holds(ct.summary(_rows(differences))["slow_bear"]) is holds


def test_a_positive_share_without_a_positive_median_does_not_hold() -> None:
    summary = {"count": 3, "positive": 2, "median_difference": 0.0}
    assert ct.config_holds(summary) is False
    assert ct.config_holds({**summary, "median_difference": 1e-12}) is True


def _sections(holding: list[bool]) -> dict[str, Any]:
    good = ct.summary(_rows([0.1, 0.1, 0.1, 0.1]))
    bad = ct.summary(_rows([-0.1, -0.1, 0.1, 0.1]))
    return {
        name: {"summary": good if ok else bad} for name, ok in zip(ct.CONFIGS, holding, strict=True)
    }


@pytest.mark.parametrize(
    ("holding", "slow_bears", "fidelity", "verdict", "reason"),
    [
        ([True, True, False], 4, True, "holds", None),
        ([True, False, False], 4, True, "fails", None),
        ([True, True, True], 3, True, "unread", "few_slow_bears"),
        ([True, True, True], 3, False, "unread", "few_slow_bears"),
        ([True, True, True], 9, False, "unread", "fidelity"),
        ([False, False, False], 9, True, "fails", None),
    ],
)
def test_the_reading_is_fixed_before_the_run(
    holding: list[bool], slow_bears: int, fidelity: bool, verdict: str, reason: str | None
) -> None:
    read = ct.reading(_sections(holding), slow_bears, fidelity)
    assert read == {
        "configs": dict(zip(ct.CONFIGS, holding, strict=True)),
        "holding": sum(holding),
        "verdict": verdict,
        "reason": reason,
    }


def test_the_whole_window_numbers() -> None:
    returns = pd.Series([-0.10, 0.05, 0.02, -0.03] * 5 + [0.01])
    assert ct.geometric(returns) == pytest.approx(
        float(np.prod(1 + returns.to_numpy()) ** (12 / 21) - 1)
    )
    level = np.cumprod(1 + returns.to_numpy())
    assert ct.max_drawdown(returns) == pytest.approx(
        float(np.min(level / np.maximum.accumulate(np.concatenate([[1.0], level]))[1:] - 1))
    )
    assert ct.max_drawdown(pd.Series([-0.1, 0.05])) == pytest.approx(-0.1)
    assert ct.tail_mean(returns) == pytest.approx(-0.10)  # ceil(0.05 x 21) = 2
    assert ct.tail_mean(pd.Series([-0.2, -0.1] + [0.0] * 18)) == pytest.approx(-0.2)
    assert ct.tail_mean(pd.Series(np.arange(21.0))) == pytest.approx(0.5)
    market = _returns([0.02, -0.05, 0.03, 0.01])
    cash = pd.Series(0.001, index=market.index)
    pair = ct.pair_returns(
        market, cash, pd.Series([1.0, 0.0, 1.0, 1.0], index=market.index), "1872-03", "1872-06"
    )
    whole = ct.whole_window(pair)
    assert whole["cost"]["turnover_per_year"] == pytest.approx(2.0 * 12 / 4)
    cost = pd.Series([0.0, 1.0, 1.0, 0.0], index=market.index) * 25 / 10_000
    assert whole["cost"]["geometric_difference"] == pytest.approx(
        ct.geometric(pair.strategy - cost) - ct.geometric(pair.benchmark)
    )
    assert whole["cost"]["bps"] == 25.0
    for key, measure in (
        ("geometric", ct.geometric),
        ("max_drawdown", ct.max_drawdown),
        ("tail_mean", ct.tail_mean),
    ):
        own, reference = measure(pair.strategy), measure(pair.benchmark)
        assert whole[key] == {
            "strategy": own,
            "benchmark": reference,
            "difference": own - reference,
        }


def test_the_data_check_counts_problems() -> None:
    inputs = ct.shiller_inputs(_frames())
    check = ct.data_check(inputs)
    assert check["ok"] and check["months"] == 696
    assert check["missing"] == {"P": 0, "D": 0, "GS10": 0}
    assert check["reproduce_error"] <= 1e-12
    frames = _frames()
    frames[ct.RATE_SOURCE] = frames[ct.RATE_SOURCE].drop(pd.Timestamp("1900-03-01"))
    frames[ct.PRICE_SOURCE].loc[pd.Timestamp("1880-01-01"), "value"] = 0.0
    frames[ct.DIVIDEND_SOURCE].loc[pd.Timestamp("1881-01-01"), "value"] = -0.1
    frames[ct.RATE_SOURCE].loc[pd.Timestamp("1890-01-01"), "value"] = 8.5
    frames[ct.RATE_SOURCE].loc[pd.Timestamp("1890-02-01"), "value"] = 1.9
    bad = ct.data_check(ct.shiller_inputs(frames))
    assert bad["missing"] == {"P": 0, "D": 0, "GS10": 1}
    assert bad["nonpositive"] == {"P": 1, "D": 1}
    assert bad["rate_out_of_range"] == 2
    assert not bad["ok"]
    edge = _frames()
    edge[ct.RATE_SOURCE].loc[pd.Timestamp("1890-01-01"), "value"] = 8.0
    edge[ct.RATE_SOURCE].loc[pd.Timestamp("1890-02-01"), "value"] = 2.0
    assert ct.data_check(ct.shiller_inputs(edge))["ok"]
    short = {
        name: frame.loc[frame.index >= pd.Timestamp("1871-02-01")]
        for name, frame in _frames().items()
    }
    assert ct.data_check(ct.shiller_inputs(short))["missing"] == {"P": 1, "D": 1, "GS10": 1}


def test_bad_frames_are_refused() -> None:
    frames = _frames()
    del frames[ct.DIVIDEND_SOURCE]
    with pytest.raises(ValueError, match="snapshot has no shiller_dividend"):
        ct.shiller_inputs(frames)
    frames = _frames()
    frames[ct.RATE_SOURCE] = frames[ct.RATE_SOURCE].rename(columns={"value": "GS10"})
    with pytest.raises(ValueError, match="shiller_gs10 frame needs a value column"):
        ct.shiller_inputs(frames)
    frames = _frames()
    price = frames[ct.PRICE_SOURCE]
    frames[ct.PRICE_SOURCE] = pd.concat([price.iloc[:5], price.iloc[4:]])
    with pytest.raises(ValueError, match="shiller_price repeats a month"):
        ct.shiller_inputs(frames)
    frames = _frames()
    frames[ct.PRICE_SOURCE].loc[pd.Timestamp("1880-01-01"), "value"] = np.nan
    with pytest.raises(ValueError, match="data check failed"):
        _run(frames)


def _daily(frames: dict[str, pd.DataFrame]) -> tuple[pd.Series, pd.Series]:
    factors = frames["ff_factors_daily"]
    return factors["Mkt-RF"] + factors["RF"], factors["RF"]


def test_the_fidelity_check_uses_the_slow_bear_probe_s_segments(result: dict[str, Any]) -> None:
    market, cash = _daily(_frames())
    found = ct.fidelity(market, cash)
    assert json.dumps(ct._clean(found)) == json.dumps(ct._clean(result["fidelity"]))
    days = market.loc[:"2010-12-31"]
    level = seg.monthly_level(seg.window_returns(days, ct.FIDELITY_WINDOW))
    items = seg.drop_segments(level)
    monthly = ((1 + days).groupby(days.index.to_period("M")).prod() - 1).to_timestamp()
    rf = (
        (1 + cash.loc[:"2010-12-31"]).groupby(days.index.to_period("M")).prod() - 1
    ).to_timestamp()
    signal = (1 + monthly).cumprod()
    for key, lag in (("month_end_lag1", 1), ("month_end_lag0", 0)):
        variant = found["variants"][key]
        assert variant["slow_bears"] == sum(item.kind == "slow_bear" for item in items)
        for name in ct.CONFIGS:
            section = ct.config_section(
                monthly,
                rf,
                ct.exposure(signal, ct.LOOKBACKS[name]),
                lag,
                "1929-01",
                "2010-12",
                items,
            )
            positive = section["summary"]["slow_bear"]["positive"]
            assert variant["positive"][name] == positive
            assert variant["gap"][name] == positive - ct.DAILY_POSITIVE[name]
    assert found["decisive"] == "average_lag1"
    assert found["holds"] is found["variants"]["average_lag1"]["within"]
    assert found["daily"] == {"slow_bears": 10, "positive": ct.DAILY_POSITIVE}


def test_the_fidelity_segments_start_in_1929() -> None:
    frames = _frames()
    factors = frames["ff_factors_daily"].copy()
    dates = factors.index.to_series()
    for first, last, total in (
        ("1928-10-01", "1928-12-31", 0.85),
        ("1929-01-01", "1929-12-31", 0.9),
    ):
        inside = dates.between(first, last)
        factors.loc[inside, "Mkt-RF"] = total ** (1 / inside.sum()) - 1 - factors.loc[inside, "RF"]
    market, cash = _daily({"ff_factors_daily": factors})
    found = ct.fidelity(market, cash)["variants"]["month_end_lag1"]
    days = market.loc[:"2010-12-31"]
    window = seg.drop_segments(seg.monthly_level(seg.window_returns(days, ct.FIDELITY_WINDOW)))
    whole = seg.drop_segments(seg.monthly_level(days))
    count = sum(item.kind == "slow_bear" for item in window)
    assert count != sum(item.kind == "slow_bear" for item in whole)
    assert found["slow_bears"] == count


def test_the_average_variant_prices_by_monthly_means() -> None:
    market, cash = _daily(_frames())
    found = ct.fidelity(market, cash)["variants"]["average_lag1"]
    days = market.loc[:"2010-12-31"]
    level = (1 + days).cumprod()
    means = level.groupby(level.index.to_period("M")).mean().to_timestamp()
    returns = means / means.shift(1) - 1
    items = [
        item for item in ct.find_segments(returns.loc["1929-01-01":]) if item.kind != "long_bull"
    ]
    rf = (
        (1 + cash.loc[:"2010-12-31"]).groupby(days.index.to_period("M")).prod() - 1
    ).to_timestamp()
    assert found["slow_bears"] == sum(item.kind == "slow_bear" for item in items)
    for name in ct.CONFIGS:
        section = ct.config_section(
            returns, rf, ct.exposure(means, ct.LOOKBACKS[name]), 1, "1929-01", "2010-12", items
        )
        assert found["positive"][name] == section["summary"]["slow_bear"]["positive"]
        assert found["summaries"][name] == section["summary"]
        assert found["summaries"][name]["long_bull"]["count"] == 0


@pytest.mark.parametrize(("shift", "within"), [(2, True), (-2, True), (3, False), (-3, False)])
def test_fidelity_holds_within_two_segments_of_the_daily_probe(
    monkeypatch: pytest.MonkeyPatch, shift: int, within: bool
) -> None:
    market, cash = _daily(_frames())
    found = ct.fidelity(market, cash)["variants"]["average_lag1"]["positive"]
    daily = {name: found[name] for name in ct.CONFIGS}
    daily["trend4_m"] += shift
    monkeypatch.setattr(ct, "DAILY_POSITIVE", daily)
    checked = ct.fidelity(market, cash)
    assert checked["variants"]["average_lag1"]["within"] is within
    assert checked["holds"] is within


@pytest.mark.parametrize("decisive", [True, False])
def test_only_the_month_mean_variant_decides(
    monkeypatch: pytest.MonkeyPatch, decisive: bool
) -> None:
    market, cash = _daily(_frames())
    variants = ct.fidelity(market, cash)["variants"]
    average, month_end = (
        variants["average_lag1"]["positive"],
        variants["month_end_lag1"]["positive"],
    )
    name = next(name for name in ct.CONFIGS if average[name] != month_end[name])
    near, far = (average, month_end) if decisive else (month_end, average)
    daily = {other: near[other] for other in ct.CONFIGS}
    daily[name] = near[name] + (2 if near[name] > far[name] else -2)
    monkeypatch.setattr(ct, "DAILY_POSITIVE", daily)
    checked = ct.fidelity(market, cash)
    assert checked["variants"]["average_lag1"]["within"] is decisive
    assert checked["variants"]["month_end_lag1"]["within"] is not decisive
    assert checked["decisive"] == "average_lag1"
    assert checked["holds"] is decisive


def test_the_fidelity_check_reads_nothing_after_2010() -> None:
    market, cash = _daily(_frames())
    spoiled = market.copy()
    spoiled.loc[spoiled.index > pd.Timestamp("2010-12-31")] = -0.5
    assert json.dumps(ct._clean(ct.fidelity(spoiled, cash))) == json.dumps(
        ct._clean(ct.fidelity(market, cash))
    )


def test_the_result_head_and_reading(result: dict[str, Any]) -> None:
    assert result["run_id"] == "TRADING-2565-41"
    assert result["owner_decision"] == ct.OWNER_DECISION
    assert result["snapshot_id"] == "sig-test"
    assert result["window"] == {"first": "1871-01", "last": "1928-12", "evaluated_from": "1872-03"}
    assert result["rules"]["signal"] == "price"
    assert result["rules"]["lookbacks"] == {
        "slow_only_m": [12],
        "fast_slow_m": [1, 12],
        "trend4_m": [1, 3, 6, 12],
    }
    assert (result["production_effect"], result["broker_action"]) == ("none", "none")
    assert set(result["sections"]) == {"main", "lag0", "cash_zero"}
    slow_bears = sum(item["kind"] == "slow_bear" for item in result["segments"])
    assert result["reading"] == ct.reading(
        result["sections"]["main"], slow_bears, result["fidelity"]["holds"]
    )
    text = ct.render_markdown(result)
    for heading in ("## 慢熊段", "## 快跌段", "## 长牛段", "## 保真核对", "## 读法（运行前写死）"):
        assert heading in text
    for title in ct.SECTION_TEXT.values():
        assert f"## 汇总：{title}" in text
    assert "读作：只印不读（保真核对不成立）" in text
    note = (
        "- 1871–1928 的信号不含股息，保真用的是总收益水平，12 个月收益的符号在价格收益落在"
        " (−股息率, 0) 的月份会不同，是说明不是判据。"
    )
    assert text.index(note) < text.index("## 段")
    rows = [line.split(" |")[0] for line in text.splitlines() if line.startswith("| 月")]
    assert rows == ["| 月均、滞后 1（判读用）", "| 月末、滞后 1", "| 月末、滞后 0"]


def test_the_cli_checks_and_writes_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    frames = _frames()
    monkeypatch.setattr(ct, "load_frames", lambda snapshot: frames)
    monkeypatch.setattr(ct, "git_provenance", lambda root: {"commit": "test"})
    runner = CliRunner()
    check = runner.invoke(ct.app, ["check"])
    assert check.exit_code == 0, check.output
    assert "1871-01..1928-12 共 696 个月；缺失 P 0 / D 0 / GS10 0" in check.output
    assert "检查通过" in check.output
    folder = tmp_path / "out"
    first = runner.invoke(ct.app, ["run", "--out", str(folder), "--snapshot", "sig-x"])
    assert first.exit_code == 0, first.output
    saved = json.loads((folder / ct.RESULT_JSON).read_text(encoding="utf-8"))
    assert saved["snapshot_id"] == "sig-x"
    assert (folder / ct.RESULT_MD).read_text(encoding="utf-8").startswith("# 1871–1928")
    assert "century_trend_check: 保真 月均、滞后 1（判读用）" in first.output
    assert "读作 只印不读（保真核对不成立）" in first.output
    again = runner.invoke(ct.app, ["run", "--out", str(folder)])
    assert again.exit_code == 1 and "拒绝覆盖" in again.output


def test_the_cli_stops_on_bad_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()

    def missing(snapshot: str) -> dict[str, pd.DataFrame]:
        raise ValueError("signal snapshot is unchecked or failed")

    monkeypatch.setattr(ct, "load_frames", missing)
    for args in (["check"], ["run", "--out", str(tmp_path / "a")]):
        out = runner.invoke(ct.app, args)
        assert out.exit_code == 1
        assert "数据载入失败：signal snapshot is unchecked or failed" in out.output
    frames = _frames()
    frames[ct.RATE_SOURCE].loc[pd.Timestamp("1890-01-01"), "value"] = 9.0
    monkeypatch.setattr(ct, "load_frames", lambda snapshot: frames)
    check = runner.invoke(ct.app, ["check"])
    assert check.exit_code == 1 and "GS10 在 2–8 以外 1 个月" in check.output
    assert "检查不通过" in check.output
    run = runner.invoke(ct.app, ["run", "--out", str(tmp_path / "b")])
    assert run.exit_code == 1 and "不能运行" in run.output
    assert not (tmp_path / "b").exists()
    del frames[ct.PRICE_SOURCE]
    gone = runner.invoke(ct.app, ["check"])
    assert gone.exit_code == 1 and "数据不能用：snapshot has no shiller_price" in gone.output


def test_rounding_of_the_tail_count() -> None:
    assert math.ceil(round(0.05 * 20, 9)) == 1
    assert ct.tail_mean(pd.Series(np.arange(20.0))) == 0.0
