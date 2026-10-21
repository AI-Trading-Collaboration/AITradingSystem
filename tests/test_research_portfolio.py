"""Acceptance tests for P1's portfolio ruler in the bench and the trial (TRADING-2565-39).

Written before the code (owner decision
``owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler``, recorded in
``docs/requirements/TRADING-2565_Research_Bench.md``). A fixed mix holding a reserve other than
cash is judged by the portfolio ruler: the bench reproduces the TRADING-2565-38 probe's numbers
exactly, the gate's portfolio rules judge the windows they name, and any other development window
records ``no_ruler``. Every test uses synthetic data.
"""

from __future__ import annotations

import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import portfolio as pf
from ai_trading_system.research_bench import segments as seg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData, from_daily_frame
from ai_trading_system.research_bench.fixed_strategy import fixed_exposure
from ai_trading_system.research_bench.prereg import (
    Rule,
    evaluate_rules,
    load_prereg,
    rule_problems,
    rule_sections,
)
from ai_trading_system.research_bench.run import RunSpec, run_strategy
from ai_trading_system.research_bench.settings import EpisodeSpec, load_settings, resolve_window
from ai_trading_system.research_bench.simulate import Rebalance, constant_targets
from ai_trading_system.research_probes import gate_false_pass as fp
from ai_trading_system.research_probes import portfolio_ruler_check as pr

FULL = load_settings()
LOADED = replace(
    FULL,
    settings=FULL.settings.model_copy(
        update={
            "falsification": FULL.settings.falsification.model_copy(
                update={"placebo_draws": 10, "bootstrap_draws": 200}
            )
        }
    ),
)
GATE_FILE = PROJECT_ROOT / "research/_trials/prereg.yaml"
DEV = resolve_window(FULL, "qqq_direction", "development")
MONTH_END = Rebalance(schedule="month_end")
DAYS = FULL.settings.trading_days_per_year
RESERVE = "treasury_10y"


def _drift(index: pd.DatetimeIndex, moves: list[tuple[str, str, float]], up: float) -> np.ndarray:
    months = index.to_period("M")
    drift = np.full(len(index), up)
    for start, end, value in moves:
        inside = (months >= pd.Period(start, freq="M")) & (months <= pd.Period(end, freq="M"))
        drift[np.asarray(inside)] = value
    return drift


def _data(seed: int = 3) -> BenchData:
    """QQQ-like returns with a long bear, a crash and short declines inside the development
    window, and no reserve until one is added."""

    index = pd.bdate_range("1997-01-02", "2011-06-30", name="date")
    moves = [
        ("1999-07", "1999-08", -0.004),
        ("2000-04", "2002-09", -0.0015),
        ("2002-10", "2004-12", 0.003),
        ("2003-06", "2003-07", -0.004),
        ("2004-04", "2004-05", -0.004),
        ("2007-11", "2008-11", -0.002),
        ("2010-05", "2010-06", -0.004),
    ]
    r = _drift(index, moves, 6e-4) + np.random.default_rng(seed).normal(0.0, 0.012, len(index))
    level = 1000.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "etf_reference_return": r,
            "cash_return": 0.0001,
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=index,
    )
    return from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "synthetic"})


BASE = _data()
FOUND = pr.window_declines(BASE.returns["qqq"], DEV)
DECLINES = tuple(EpisodeSpec(id=s.id, start=s.start, end=s.end) for s in FOUND)


def _with_reserve(family: str, seed_index: int = 0) -> tuple[BenchData, pd.Series]:
    seed = fp.seed_list(seed_index + 1)[seed_index]
    reserve = pr.reserve_returns(BASE.returns, pr.FAMILIES[family], seed, FOUND, DAYS)
    return replace(BASE, reserves={RESERVE: reserve.rename(RESERVE)}), reserve


def _spec(weight: float = 0.6, reserve: str = RESERVE) -> RunSpec:
    return RunSpec(
        question_type="qqq_direction",
        strategy_id=f"mix{weight:g}",
        params={"exposure": weight},
        window_role="development",
        rebalance=MONTH_END,
        reserve=reserve,
    )


# The bench reproduces the probe ------------------------------------------------------------------


def test_the_declines_are_the_probe_declines() -> None:
    level = seg.monthly_level(seg.window_returns(BASE.returns["qqq"], DEV))
    assert seg.decline_segments(level) == pr.decline_segments(level)
    assert seg.declines(seg.window_returns(BASE.returns["qqq"], DEV)) == FOUND
    assert len(FOUND) >= 4 and all(s.kind == "decline" for s in FOUND)
    assert seg.DECLINE_LINE == pr.DECLINE_LINE == 0.10


def test_a_fall_of_exactly_the_line_opens_a_decline() -> None:
    # 1.0 * (1 - 0.10) is the float 0.9: the first fall reaches the line exactly, the second
    # stops a hair above it.
    months = pd.period_range("2001-01", periods=7, freq="M")
    level = pd.Series([1.0, 0.95, 0.9, 0.95, 1.0, 0.9 + 1e-12, 1.0], index=months)
    found = seg.decline_segments(level)
    assert found == pr.decline_segments(level)
    assert [(s.id, s.start, s.end, s.months) for s in found] == [
        ("decline_200101", "2001-02", "2001-03", 2)
    ]


def test_the_split_keeps_the_six_month_line_of_the_drops() -> None:
    # drop_segments now shares its loop with the declines; six months from high to trough is
    # still a slow bear and five a fast drop (TRADING-2565-36).
    six = [1.0, 1.1, 1.0, 0.9, 0.85, 0.8, 0.75, 0.7, 0.8, 0.9, 1.0, 1.2]
    five = [1.0, 1.1, 1.0, 0.95, 0.9, 0.85, 0.7, 0.9, 1.2]
    for values, expected in ((six, ("slow_bear", 6)), (five, ("fast_drop", 5))):
        level = pd.Series(values, index=pd.period_range("2000-01", periods=len(values), freq="M"))
        assert [(s.kind, s.months) for s in seg.drop_segments(level)] == [expected]


def test_the_decline_check_compares_declines_only() -> None:
    found = [
        seg.Segment(
            "decline_200101", "decline", "2001-02", "2001-03", "2001-01", "2001-03", 2, -0.1
        ),
        seg.Segment(
            "slow_bear_200101", "slow_bear", "2001-02", "2001-09", "2001-01", "2001-09", 8, -0.3
        ),
    ]
    listed = [EpisodeSpec(id="decline_200101", start="2001-02", end="2001-03")]
    assert seg.decline_problems(found, listed) == []


@pytest.mark.parametrize(("family", "weight"), [("hedge_0.3", 0.6), ("null_carry", 0.5)])
def test_the_portfolio_section_is_the_probe_measure(family: str, weight: float) -> None:
    data, reserve = _with_reserve(family)
    result = run_strategy(
        data,
        fixed_exposure,
        _spec(weight),
        LOADED,
        falsification_confidence=0.95,
        declines=DECLINES,
        portfolio_weight=weight,
    )
    targets = constant_targets(data.inputs.index, weight)
    probe = pr.measure(data, targets, reserve, DEV, MONTH_END, weight, FOUND, LOADED)
    section = result.portfolio
    assert section is not None

    assert section["weight"] == weight
    assert section["legs"] == probe["legs"]
    assert section["geometric_difference"] == probe["geometric"]
    assert result.comparison["geometric_annual_return"] == probe["geometric"]
    assert result.falsification is not None
    assert result.falsification["cost_25"]["geometric_annual_return"] == probe["geometric_cost"]
    for key, other in (
        ("reserve_excess", "reserve_excess"),
        ("rebalance_return", "rebalancing"),
        ("excess_annual", "excess_annual"),
        ("quiet_excess", "quiet_excess"),
        ("quiet_days", "quiet_days"),
        ("quiet_fallback", "quiet_fallback"),
    ):
        assert section[key] == probe[key if other == key else other], key
    assert len(section["segments"]) == len(probe["segments"])
    for row, expected in zip(section["segments"], probe["segments"], strict=True):
        assert {key: row[key] for key in expected} == expected
        assert (row["start"], row["end"]) == next(
            (s.start, s.end) for s in FOUND if s.id == row["id"]
        )
    summary = probe["summary"]
    assert (
        section["segment_count"],
        section["positive_count"],
        section["positive_share"],
        section["median_ratio"],
        section["min_ratio"],
        section["excluded"],
    ) == (
        summary["count"],
        summary["positive"],
        summary["positive_share"],
        summary["median_ratio"],
        summary["min_ratio"],
        summary["excluded"],
    )


def test_every_rule_field_is_a_number_of_the_section() -> None:
    data, _ = _with_reserve("hedge_0.3")
    result = run_strategy(
        data, fixed_exposure, _spec(), LOADED, declines=DECLINES, portfolio_weight=0.6
    )
    assert result.portfolio is not None
    assert rule_sections(FULL.settings)["portfolio"] == pf.PORTFOLIO_FIELDS
    for field in pf.PORTFOLIO_FIELDS:
        value = result.portfolio[field]
        assert isinstance(value, int | float) and not isinstance(value, bool), field


def test_the_section_stays_out_of_the_fingerprint() -> None:
    data, _ = _with_reserve("hedge_0.3")
    plain = run_strategy(data, fixed_exposure, _spec(), LOADED)
    ruled = run_strategy(
        data, fixed_exposure, _spec(), LOADED, declines=DECLINES, portfolio_weight=0.6
    )

    assert plain.portfolio is None and ruled.portfolio is not None
    a, b = plain.to_dict(), ruled.to_dict()
    assert a.pop("portfolio") is None and b.pop("portfolio") == ruled.portfolio
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    assert plain.fingerprint == ruled.fingerprint


def test_the_ruler_needs_a_reserve_and_the_declines() -> None:
    data, _ = _with_reserve("hedge_0.3")
    with pytest.raises(ValueError, match="reserve other than cash"):
        run_strategy(
            BASE,
            fixed_exposure,
            _spec(reserve="cash"),
            LOADED,
            declines=DECLINES,
            portfolio_weight=0.6,
        )
    with pytest.raises(ValueError, match="declines"):
        run_strategy(data, fixed_exposure, _spec(), LOADED, portfolio_weight=0.6)
    # Declines alone, without a weight, ask for nothing.
    assert run_strategy(data, fixed_exposure, _spec(), LOADED, declines=DECLINES).portfolio is None
    # The ruler itself refuses an empty list too, whoever calls it.
    with pytest.raises(ValueError, match="the portfolio ruler needs the window's declines"):
        pf.portfolio_section(
            SimpleNamespace(),
            SimpleNamespace(),
            {},
            {},
            data.returns,
            data.returns["cash"],
            0.6,
            (),
            DAYS,
        )


def test_the_quiet_excess_falls_back_to_the_whole_window_below_half_the_days() -> None:
    data, _ = _with_reserve("null_carry")
    # One decline covering the whole window leaves no quiet day.
    everything = (EpisodeSpec(id="decline_199902", start="1999-03", end="2010-12"),)
    section = run_strategy(
        data, fixed_exposure, _spec(), LOADED, declines=everything, portfolio_weight=0.6
    ).portfolio
    assert section is not None
    assert (section["quiet_days"], section["quiet_fallback"]) == (0, True)
    assert section["quiet_excess"] == section["excess_annual"]


@pytest.mark.parametrize(("end", "fallback"), [("2004-08", False), ("2005-09", True)])
def test_the_fallback_line_is_half_the_return_days(end: str, fallback: bool) -> None:
    # One decline from the window's first month: 66 of 142 months leave 54% of the days quiet,
    # 79 months leave 44%.
    data, reserve = _with_reserve("null_carry")
    decline = (EpisodeSpec(id="decline_199902", start="1999-03", end=end),)
    section = run_strategy(
        data, fixed_exposure, _spec(), LOADED, declines=decline, portfolio_weight=0.6
    ).portfolio
    assert section is not None
    assert section["quiet_fallback"] is fallback
    assert (section["quiet_excess"] == section["excess_annual"]) is fallback
    found = [seg.Segment("decline_199902", "decline", "1999-03", end, None, None, 0, None)]
    targets = constant_targets(data.inputs.index, 0.6)
    probe = pr.measure(data, targets, reserve, DEV, MONTH_END, 0.6, found, LOADED)
    assert (section["quiet_days"], section["quiet_excess"]) == (
        probe["quiet_days"],
        probe["quiet_excess"],
    )


def test_a_decline_where_the_cash_mix_loses_little_is_listed_not_counted() -> None:
    data, _ = _with_reserve("hedge_0.3")
    calm = EpisodeSpec(id="decline_200501", start="2005-02", end="2005-02")
    section = run_strategy(
        data,
        fixed_exposure,
        _spec(),
        LOADED,
        declines=(*DECLINES, calm),
        portfolio_weight=0.6,
    ).portfolio
    assert section is not None
    row = section["segments"][-1]
    if row["benchmark_return"] >= pf.SMALL_LOSS:
        assert (row["counted"], row["ratio"]) == (False, None)
        assert "decline_200501" in section["excluded"]
    else:  # The noise made it a loss: it counts like any other.
        assert row["counted"] and row["ratio"] == row["cushion"] / abs(row["benchmark_return"])


def test_a_decline_counts_when_the_cash_mix_loses_more_than_two_percent() -> None:
    dates = pd.bdate_range("2005-01-03", "2005-06-30")
    bench = pd.Series(0.0, index=dates)
    # One day carries each month's move. Exactly 2% cannot be reached: 1 - 0.02 - 1 is a float
    # below -0.02, so the line is checked on both sides of it.
    moves = {"2005-02": -0.025, "2005-03": -0.0201, "2005-04": -0.0199}
    for month, move in moves.items():
        bench[dates[dates.strftime("%Y-%m") == month][0]] = move
    declines = tuple(
        EpisodeSpec(id=f"decline_{month.replace('-', '')}", start=month, end=month)
        for month in moves
    )
    flat = pd.Series(0.0, index=dates)
    rows = pf.cushion_rows(
        SimpleNamespace(returns=bench * 0.5),
        SimpleNamespace(returns=bench),
        flat,
        flat,
        0.6,
        0.0,
        declines,
    )
    assert [row["counted"] for row in rows] == [True, True, False]
    assert [row["ratio"] for row in rows][:2] == pytest.approx([0.5, 0.5])
    assert rows[2]["ratio"] is None
    summary = pf.cushion_summary(rows)
    assert (summary["segment_count"], summary["excluded"]) == (2, ["decline_200504"])


def test_a_decline_with_no_cushion_is_not_cushioned() -> None:
    rows = [
        {"id": "decline_a", "ratio": 0.0, "counted": True},
        {"id": "decline_b", "ratio": 0.2, "counted": True},
        {"id": "decline_c", "ratio": None, "counted": False},
    ]
    summary = pf.cushion_summary(rows)
    assert (summary["segment_count"], summary["positive_count"]) == (2, 1)
    assert summary["positive_share"] == 0.5


# Rules -------------------------------------------------------------------------------------------


def test_a_missing_section_fails_a_rule_without_an_error() -> None:
    rule = Rule(metric="portfolio.median_ratio", op=">=", value=0.1)
    assert evaluate_rules((rule,), {"portfolio": None}) == [
        {
            "metric": "portfolio.median_ratio",
            "op": ">=",
            "value": 0.1,
            "observed": None,
            "holds": False,
        }
    ]


def test_a_question_cannot_rule_on_the_portfolio_yet() -> None:
    prereg = load_prereg(PROJECT_ROOT / "research/QQQ-Q5/prereg.yaml")
    raw = prereg.model_dump()
    strategy = raw["validation"]["strategies"][0]
    raw["validation"]["rules"][strategy] = [
        *raw["validation"]["rules"][strategy],
        {"metric": "portfolio.median_ratio", "op": ">", "value": 0.0, "note": ""},
    ]
    changed = type(prereg).model_validate(raw)
    assert rule_problems(prereg, FULL.settings) == []
    assert rule_problems(changed, FULL.settings) == [
        f"{strategy}: portfolio.median_ratio is computed only in trials"
    ]


# The gate's portfolio fields ---------------------------------------------------------------------


def _gate() -> dict[str, Any]:
    return copy.deepcopy(yaml.safe_load(GATE_FILE.read_text(encoding="utf-8")))


def test_the_gate_names_the_ruler_windows_and_lists_their_declines() -> None:
    gate = tr.load_gate(GATE_FILE)
    us, qqq = gate.markets["us_market"], gate.markets["qqq"]

    assert us.portfolio_rules is not None and qqq.portfolio_rules is not None
    assert us.portfolio_rules.windows == ("us_market_1929_2010",)
    assert qqq.portfolio_rules.windows == ("qqq_long_history_development",)
    assert us.portfolio_rules_for("us_market_1929_2010") == us.portfolio_rules.rules
    assert us.portfolio_rules_for("us_market_1962_2010") is None
    assert {window: len(items) for window, items in (us.decline_episodes or {}).items()} == {
        "us_market_1929_2010": 25,
        "us_market_1962_2010": 15,
    }
    assert len(qqq.decline_list("qqq_long_history_development")) == 6
    assert qqq.decline_list("us_market_1929_2010") == ()
    assert tr.gate_problems(gate, FULL) == []


@pytest.mark.parametrize(
    ("change", "match"),
    [
        (lambda g: g["markets"]["qqq"]["portfolio_rules"].update(windows=[]), "at least 1"),
        (
            lambda g: g["markets"]["qqq"]["portfolio_rules"].update(
                windows=["qqq_long_history_development", "qqq_long_history_development"]
            ),
            "unique",
        ),
        (lambda g: g["markets"]["qqq"]["portfolio_rules"].update(rules=[]), "at least 1"),
        (lambda g: g["markets"]["qqq"]["portfolio_rules"].update(extra=1), "Extra inputs"),
        (
            lambda g: g["markets"]["qqq"]["decline_episodes"][
                "qqq_long_history_development"
            ].append({"id": "decline_200801", "start": "2008-02", "end": "2008-03"}),
            "overlap",
        ),
        # Sharing a month is overlapping: decline_201004 ends in 2010-06.
        (
            lambda g: g["markets"]["qqq"]["decline_episodes"][
                "qqq_long_history_development"
            ].append({"id": "decline_201006", "start": "2010-06", "end": "2010-07"}),
            "overlap",
        ),
        (
            lambda g: g["markets"]["qqq"]["decline_episodes"][
                "qqq_long_history_development"
            ].append({"id": "decline_200003", "start": "2011-02", "end": "2011-03"}),
            "repeat",
        ),
    ],
)
def test_malformed_portfolio_fields_are_refused(change: Any, match: str) -> None:
    raw = _gate()
    change(raw)
    with pytest.raises(ValueError, match=match):
        tr.TrialGate.model_validate(raw)


def test_portfolio_fields_no_run_can_use_are_gate_problems() -> None:
    raw = _gate()
    us = raw["markets"]["us_market"]
    us["portfolio_rules"]["windows"] = ["us_market_1929_2010", "us_market_1962_2010", "nowhere"]
    us["portfolio_rules"]["rules"].append({"metric": "portfolio.no_such", "op": ">", "value": 0})
    us["decline_episodes"]["qqq_long_history_development"] = [
        {"id": "decline_200003", "start": "2000-04", "end": "2002-09"}
    ]
    del us["decline_episodes"]["us_market_1962_2010"]
    problems = tr.gate_problems(tr.TrialGate.model_validate(raw), FULL)

    assert problems == [
        "us_market: no metric portfolio.no_such",
        "us_market: declines listed for qqq_long_history_development, "
        "not a development window here",
        "us_market: portfolio rules for nowhere, not a development window here",
        "us_market: portfolio rules for us_market_1962_2010, which lists no declines",
        "us_market: portfolio rules for nowhere, which lists no declines",
    ]


def test_only_a_fixed_mix_with_a_reserve_uses_the_ruler() -> None:
    def config(function: str, reserve: str) -> tr.TrialConfig:
        params: dict[str, Any] = {"exposure": 0.6} if function == "fixed_exposure" else {}
        return tr.TrialConfig(id="x", function=function, params=params, reserve=reserve)

    assert tr.uses_portfolio_ruler(config("fixed_exposure", RESERVE))
    assert not tr.uses_portfolio_ruler(config("fixed_exposure", "cash"))
    assert not tr.uses_portfolio_ruler(config("signal_exposure", RESERVE))


# Verdicts and the report -------------------------------------------------------------------------


def _record(market: str, holds: bool | None, outcome: str | None = None) -> dict[str, Any]:
    record: dict[str, Any] = {
        "trial_id": "T0099",
        "config": "mix",
        "market": market,
        "status": "ok",
        "gate_holds": holds,
    }
    if outcome is not None:
        record["gate_outcome"] = outcome
    return record


@pytest.mark.parametrize(
    ("records", "verdict"),
    [
        ([_record("us_market", None, "no_ruler"), _record("qqq", True, "pass")], "no_ruler"),
        ([_record("us_market", None, "no_ruler")], "no_ruler"),
        ([_record("us_market", None, "no_ruler"), _record("qqq", False, "fail")], "fail"),
        ([_record("us_market", True, "pass"), _record("qqq", True, "pass")], "pass"),
        ([_record("us_market", True, "pass")], "incomplete"),
        # Records written before the ruler have no outcome and keep their verdicts.
        ([_record("us_market", False), _record("qqq", True)], "fail"),
        ([_record("us_market", True), _record("qqq", True)], "pass"),
        ([_record("qqq", True)], "incomplete"),
        # Only the first successful run on each market counts.
        (
            [_record("us_market", None, "no_ruler"), _record("us_market", False, "fail")],
            "no_ruler",
        ),
    ],
)
def test_no_ruler_is_neither_a_pass_nor_a_failure(
    records: list[dict[str, Any]], verdict: str
) -> None:
    assert tr.primary_verdict(records, "T0099", "mix") == verdict
    assert verdict in tr.VERDICT_NAMES


def _run_view(**changes: Any) -> dict[str, Any]:
    view: dict[str, Any] = {
        "rule_set": "portfolio",
        "gate_outcome": "no_ruler",
        "gate": [],
        "gate_readonly": [
            {
                "metric": "comparison.max_drawdown",
                "op": ">=",
                "value": 0.05,
                "observed": 0.04,
                "holds": False,
            }
        ],
        "window_id": "us_market_1962_2010",
        "portfolio": {
            "weight": 0.6,
            "geometric_difference": 0.008,
            "reserve_excess": 0.007,
            "rebalance_return": 0.001,
            "excess_annual": 0.0175,
            "quiet_excess": 0.012,
            "quiet_days": 9000,
            "quiet_fallback": False,
            "segment_count": 2,
            "positive_count": 1,
            "positive_share": 0.5,
            "median_ratio": 0.05,
            "min_ratio": -0.2,
            "excluded": ["decline_197808"],
            "segments": [
                {
                    "id": "decline_196811",
                    "start": "1968-12",
                    "end": "1970-06",
                    "months": 19,
                    "strategy_return": -0.222,
                    "benchmark_return": -0.178,
                    "difference": -0.044,
                    "cushion": -0.052,
                    "ratio": -0.2921,
                    "reserve_excess": -0.08,
                    "counted": True,
                }
            ],
        },
    }
    view.update(changes)
    return view


def test_the_portfolio_block_prints_the_ruler_and_the_timing_rules_read_only() -> None:
    lines = tr.portfolio_lines(_run_view())

    assert lines[0] == (
        "组合尺子（储备配置按它判；owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler）"
    )
    assert (
        "- 判定：没有尺子。门的组合尺子没有 us_market_1962_2010 这个窗口的门槛，"
        "不算过，也不算没过，这个配置过不了；下面的数照印。"
    ) in lines
    assert (
        "- 分解：几何差 +0.80 点 = 储备自身超额（全窗口）+0.70 点 + 再平衡收益 +0.10 点"
        "（QQQ 权重 0.60）"
    ) in lines
    assert (
        "- 储备对现金：全窗口超额 +1.75 点/年；平时超额（段外）+1.20 点/年（段外 9000 天）" in lines
    )
    assert (
        "- 下跌段计入 2 段：r 中位 +0.050，r > 0 的 1 段（50.0%），最小 -0.200；"
        "不计入（B 段内跌不到 2%）：decline_197808"
    ) in lines
    assert "| 段 | 月份 | 月数 | B 段内 | A 段内 | ΔD（点） | C（点） | r | 储备段内超额 |" in lines
    assert (
        "| decline_196811 | 1968-12..1970-06 | 19 | -17.8% | -22.2% | -4.4 | -5.2 | -0.292 "
        "| -8.0% |" in lines
    )
    at = lines.index("择时尺子（只读，不进判定）")
    assert lines[at + 2 : at + 5] == [
        "| 规则 | 阈值 | 实际 | 成立 |",
        "|---|---|---|---|",
        "| comparison.max_drawdown | >= 0.05 | 0.0400 | 否 |",
    ]


def test_a_judged_window_prints_the_portfolio_rules_and_a_fallback_is_named() -> None:
    rows = [
        {
            "metric": "portfolio.median_ratio",
            "op": ">=",
            "value": 0.1,
            "observed": 0.05,
            "holds": False,
        }
    ]
    view = _run_view(gate_outcome="fail", gate=rows, window_id="us_market_1929_2010")
    view["portfolio"] = {**view["portfolio"], "quiet_fallback": True, "excluded": []}
    lines = tr.portfolio_lines(view)

    assert not any(line.startswith("- 判定：没有尺子") for line in lines)
    assert lines[2:5] == [
        "| 规则 | 阈值 | 实际 | 成立 |",
        "|---|---|---|---|",
        "| portfolio.median_ratio | >= 0.1 | 0.0500 | 否 |",
    ]
    assert any(line.endswith("（段外 9000 天，不到一半，用全窗口超额）") for line in lines)
    assert any(line.endswith("不计入（B 段内跌不到 2%）：无") for line in lines)
