"""Invariants of P1's portfolio ruler (TRADING-2565-39).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler``). All data is synthetic, on a
calendar of exactly 21 trading days a month (252 a year, the bench's annualisation).

59. The decomposition is exact: with the reserve equal to cash every number of the ruler is zero;
    the geometric difference is always the reserve's own excess plus the rebalancing return; the
    rebalancing return is positive for a reserve independent of the market (daily rebalancing, no
    cost) and negative for one that moves with the market.
60. The quiet excess removes carry and nothing else: a reserve earning a constant 2% a year over
    cash has a quiet excess and a whole-window excess of 2% compounded (within 0.001), and no
    decline cushions it (C <= 0); adding that carry to a noise stream changes each counted
    decline's ratio r by -0.05 to +0.001 against the same stream alone (the compounding term is
    never positive).
61. The declines are the rule's: the bench's drop rule at 10% with no minimum length, opened by a
    fall to 90% of the running high, labelled by the high's month and run from the month after
    the high to the trough; the slow-bear segments are unchanged; the gate lists exactly the
    probe's declines (TRADING-2565-38) of each development window; differences are named.
62. The gate changes only for reserve configurations: the timing rules are word for word, the
    portfolio rules are the requirements' stage 1 (t = 0.10, q = 0.75, n >= 4) on
    us_market_1929_2010 only and stage 2 on QQQ; the 1962 window is not named and its reserve
    mapping stays; a named window needs its declines.
63. Frozen things stay frozen: the plans T0001-T0007 and the probes' gate copies byte for byte,
    the reserves, and the verdict of every record written before the ruler.
"""

from __future__ import annotations

import itertools
from collections.abc import Sequence
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import segments as seg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import RESERVES, BenchData, from_daily_frame
from ai_trading_system.research_bench.fixed_strategy import fixed_exposure
from ai_trading_system.research_bench.run import RunSpec, run_strategy
from ai_trading_system.research_bench.settings import EpisodeSpec, load_settings, resolve_window
from ai_trading_system.research_bench.simulate import Rebalance

LOADED = load_settings()
DAYS = LOADED.settings.trading_days_per_year
DEV = resolve_window(LOADED, "qqq_direction", "development")
GATE_FILE = PROJECT_ROOT / "research/_trials/prereg.yaml"
PROBES = PROJECT_ROOT / "src/ai_trading_system/research_probes"
DAILY = Rebalance(schedule="daily")
MONTH_END = Rebalance(schedule="month_end")
RESERVE = "treasury_10y"
CARRY = 0.02
NOISE = 0.07
# Monthly moves of the planted market: a long bear, a crash, shallow declines and long rises.
PLANTED = [
    ("1997-01", "2000-03", 0.015),
    ("2000-04", "2001-09", -0.03),
    ("2001-10", "2003-02", 0.02),
    ("2003-03", "2003-04", -0.08),
    ("2003-05", "2006-05", 0.012),
    ("2006-06", "2006-06", -0.12),
    ("2006-07", "2007-10", 0.015),
    ("2007-11", "2008-11", -0.05),
    ("2008-12", "2010-04", 0.02),
    ("2010-05", "2010-06", -0.07),
    ("2010-07", "2011-06", 0.015),
]


def _calendar() -> pd.DatetimeIndex:
    """The first 21 days of each month: every month and year weigh the same."""
    months = pd.period_range("1997-01", "2011-06", freq="M")
    days = [month.start_time + pd.Timedelta(days=day) for month in months for day in range(21)]
    return pd.DatetimeIndex(days, name="date")


CALENDAR = _calendar()


def _market(seed: int = 7) -> pd.Series:
    months = CALENDAR.to_period("M")
    out = pd.Series(0.0, index=CALENDAR)
    for first, last, move in PLANTED:
        inside = (months >= pd.Period(first, freq="M")) & (months <= pd.Period(last, freq="M"))
        out[inside] = (1.0 + move) ** (1.0 / 21) - 1.0
    noise = np.random.default_rng(seed).normal(0.0, 0.003, len(CALENDAR))
    return out + noise - noise.mean()


def _data(reserve: pd.Series | None = None) -> BenchData:
    returns = MARKET.to_numpy()
    level = 1000.0 * np.cumprod(1.0 + returns)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": returns,
            "etf_reference_return": returns,
            "cash_return": 0.0001,
            "lev2_return": 2 * returns,
            "lev3_return": 3 * returns,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=CALENDAR,
    )
    data = from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "synthetic"})
    if reserve is None:
        return data
    return BenchData(
        inputs=data.inputs,
        returns=data.returns,
        source=data.source,
        reserves={RESERVE: reserve.rename(RESERVE)},
    )


MARKET = _market()
BASE = _data()
CASH = BASE.returns["cash"]
FOUND = seg.declines(seg.window_returns(BASE.returns["qqq"], DEV))
DECLINES = tuple(EpisodeSpec(id=s.id, start=s.start, end=s.end) for s in FOUND)


def _noise(stream: int) -> pd.Series:
    draws = np.random.default_rng([59, stream]).standard_normal(len(CALENDAR))
    return pd.Series(draws * NOISE / np.sqrt(DAYS), index=CALENDAR)


def _ruler(
    reserve: pd.Series,
    weight: float = 0.6,
    rebalance: Rebalance = MONTH_END,
    cost_bps: float | None = None,
) -> dict[str, Any]:
    spec = RunSpec(
        question_type="qqq_direction",
        strategy_id="mix",
        params={"exposure": weight},
        window_role="development",
        rebalance=rebalance,
        cost_bps=cost_bps,
        reserve=RESERVE,
    )
    result = run_strategy(
        _data(reserve),
        fixed_exposure,
        spec,
        LOADED,
        declines=DECLINES,
        portfolio_weight=weight,
    )
    assert result.portfolio is not None
    return result.portfolio


def test_the_planted_market_has_counted_declines_of_every_length() -> None:
    assert len(CALENDAR) == 21 * 12 * 14 + 21 * 6
    months = sorted({s.months for s in FOUND})
    assert len(FOUND) >= 5 and months[0] <= 2 and months[-1] >= 12


# 59. The decomposition is exact ------------------------------------------------------------------


def test_a_cash_reserve_has_every_number_zero() -> None:
    section = _ruler(CASH.copy(), rebalance=DAILY)

    assert section["legs"]["reserve"] == section["legs"]["cash"]
    for key in (
        "geometric_difference",
        "reserve_excess",
        "rebalance_return",
        "excess_annual",
        "quiet_excess",
        "median_ratio",
        "min_ratio",
    ):
        assert section[key] == pytest.approx(0.0, abs=1e-12), key
    # Exposure averages may differ in the last bit, so a ratio can be a rounding error above 0.
    for row in section["segments"]:
        for key in ("difference", "cushion", "reserve_excess"):
            assert row[key] == pytest.approx(0.0, abs=1e-12), (row["id"], key)
        assert row["ratio"] is None or row["ratio"] == pytest.approx(0.0, abs=1e-10)


@pytest.mark.parametrize("weight", [0.4, 0.6, 0.8])
@pytest.mark.parametrize("rebalance", [DAILY, MONTH_END])
def test_the_geometric_difference_is_own_excess_plus_rebalancing(
    weight: float, rebalance: Rebalance
) -> None:
    for reserve in (
        CASH + CARRY / DAYS + _noise(1),
        CASH + 0.5 * (MARKET - CASH) + _noise(2) / 3,
    ):
        section = _ruler(reserve, weight, rebalance)
        total = section["reserve_excess"] + section["rebalance_return"]
        assert section["geometric_difference"] == pytest.approx(total, abs=1e-12)
        legs = section["legs"]
        assert section["reserve_excess"] == pytest.approx(
            (1.0 - weight) * (legs["reserve"] - legs["cash"]), abs=1e-15
        )


def test_rebalancing_earns_on_an_independent_reserve_and_loses_on_a_correlated_one() -> None:
    for stream in (3, 4, 5):
        independent = _ruler(CASH + _noise(stream), rebalance=DAILY, cost_bps=0.0)
        assert independent["rebalance_return"] > 0, stream
    correlated = _ruler(CASH + 0.5 * (MARKET - CASH) + _noise(6) / 3, rebalance=DAILY, cost_bps=0.0)
    assert correlated["rebalance_return"] < 0


# 60. The quiet excess removes carry and nothing else ----------------------------------------------


def test_a_constant_carry_is_removed_and_cushions_nothing() -> None:
    section = _ruler(CASH + CARRY / DAYS)
    compounded = (1.0 + CARRY / DAYS) ** DAYS - 1.0

    assert section["quiet_fallback"] is False
    assert section["quiet_excess"] == pytest.approx(compounded, abs=1e-3)
    assert section["excess_annual"] == pytest.approx(compounded, abs=1e-3)
    assert section["reserve_excess"] == pytest.approx(0.4 * compounded, abs=1e-3)
    assert section["segment_count"] >= 4
    for row in section["segments"]:
        assert row["cushion"] <= 0.0, row["id"]


@pytest.mark.parametrize("stream", [11, 12, 13, 14])
def test_carry_moves_each_ratio_by_the_compounding_term_only(stream: int) -> None:
    noise = _noise(stream)
    carry = _ruler(CASH + CARRY / DAYS + noise)
    alone = _ruler(CASH + noise)

    rows = zip(carry["segments"], alone["segments"], strict=True)
    counted = [(a, b) for a, b in rows if a["counted"]]
    assert counted and all(b["counted"] for _, b in counted)
    for a, b in counted:
        assert -0.05 <= a["ratio"] - b["ratio"] <= 0.001, a["id"]


# 61. The declines are the rule's ------------------------------------------------------------------

# The ruler probe's declines (TRADING-2565-38, main 91ff36bd, run on 2026-10-21): ids name the
# high's month; a decline runs from the month after the high to the trough.
US_1929 = [
    ("decline_192908", "1929-09", "1932-06"),
    ("decline_193208", "1932-09", "1933-02"),
    ("decline_193308", "1933-09", "1933-10"),
    ("decline_193401", "1934-02", "1934-07"),
    ("decline_193702", "1937-03", "1938-03"),
    ("decline_193812", "1939-01", "1939-04"),
    ("decline_193909", "1939-10", "1942-04"),
    ("decline_194605", "1946-06", "1947-05"),
    ("decline_194805", "1948-06", "1949-05"),
    ("decline_195707", "1957-08", "1957-12"),
    ("decline_196112", "1962-01", "1962-06"),
    ("decline_196601", "1966-02", "1966-09"),
    ("decline_196811", "1968-12", "1970-06"),
    ("decline_197212", "1973-01", "1974-09"),
    ("decline_197506", "1975-07", "1975-09"),
    ("decline_197808", "1978-09", "1978-10"),
    ("decline_198001", "1980-02", "1980-03"),
    ("decline_198011", "1980-12", "1982-07"),
    ("decline_198306", "1983-07", "1984-05"),
    ("decline_198708", "1987-09", "1987-11"),
    ("decline_199005", "1990-06", "1990-10"),
    ("decline_199806", "1998-07", "1998-08"),
    ("decline_200003", "2000-04", "2002-09"),
    ("decline_200710", "2007-11", "2009-02"),
    ("decline_201004", "2010-05", "2010-06"),
]
PROBE_DECLINES = {
    "us_market": {
        "us_market_1929_2010": US_1929,
        # The 1962 window starts after the 1961 high: its first decline opens at 1962-02.
        "us_market_1962_2010": [("decline_196202", "1962-03", "1962-06"), *US_1929[11:]],
    },
    "qqq": {
        "qqq_long_history_development": [
            ("decline_200003", "2000-04", "2002-09"),
            ("decline_200211", "2002-12", "2002-12"),
            ("decline_200412", "2005-01", "2005-04"),
            ("decline_200601", "2006-02", "2006-07"),
            ("decline_200710", "2007-11", "2009-02"),
            ("decline_201004", "2010-05", "2010-06"),
        ]
    },
}


def _level(values: Sequence[float]) -> pd.Series:
    return pd.Series(values, index=pd.period_range("2000-01", periods=len(values), freq="M"))


def test_a_decline_is_any_fall_to_ninety_percent_of_the_running_high() -> None:
    # High 100 in 2000-02; 90 is exactly 10% down (opens); trough 85 before 100 returns. Then a
    # one-month fall from 110 to 98.9 (opens, no minimum length) and a 9.9% fall that does not.
    level = _level([95, 100, 90, 85, 95, 100, 110, 98.9, 110, 120, 108.2, 119])
    found = seg.decline_segments(level)

    assert found == [
        seg.Segment(
            "decline_200002",
            "decline",
            "2000-03",
            "2000-04",
            "2000-02",
            "2000-04",
            2,
            85 / 100 - 1.0,
        ),
        seg.Segment(
            "decline_200007",
            "decline",
            "2000-08",
            "2000-08",
            "2000-07",
            "2000-08",
            1,
            98.9 / 110 - 1.0,
        ),
    ]
    assert [(s.peak, s.trough) for s in found] == [
        (s.peak, s.trough) for s in seg.drop_segments(level, 0.10)
    ]


def test_the_slow_bear_rule_is_unchanged() -> None:
    # A seven-month fall of 22% is a slow bear; a one-month fall of 21% is a fast drop.
    level = _level([100, 95, 90, 85, 82, 80, 79, 78, 90, 101, 80, 105])
    expected = [
        seg.Segment(
            "slow_bear_200001",
            "slow_bear",
            "2000-02",
            "2000-08",
            "2000-01",
            "2000-08",
            7,
            78 / 100 - 1.0,
        ),
        seg.Segment(
            "fast_drop_200010",
            "fast_drop",
            "2000-11",
            "2000-11",
            "2000-10",
            "2000-11",
            1,
            80 / 101 - 1.0,
        ),
    ]
    assert seg.drop_segments(level) == seg.drop_segments(level, 0.20, 6) == expected
    assert [s.kind for s in seg.drop_segments(level, 0.20, 8)] == ["fast_drop", "fast_drop"]
    assert (seg.DROP, seg.SLOW_MONTHS, seg.LONG_BULL_MONTHS, seg.DECLINE_LINE) == (
        0.20,
        6,
        36,
        0.10,
    )


def test_the_gate_lists_the_probe_declines_of_each_development_window() -> None:
    gate = tr.load_gate(GATE_FILE)
    for market, windows in PROBE_DECLINES.items():
        entry = gate.markets[market]
        developed = {
            resolve_window(LOADED, question_type, "development").window_id
            for question_type in entry.question_types()
        }
        assert set(entry.decline_episodes or {}) == set(windows) == developed
        for window_id, expected in windows.items():
            listed = [(e.id, e.start, e.end) for e in entry.decline_list(window_id)]
            assert listed == expected, window_id
            for id_, start, _ in listed:
                assert id_ == f"decline_{(pd.Period(start, freq='M') - 1).strftime('%Y%m')}"


def test_recomputing_the_declines_agrees_and_names_any_difference() -> None:
    assert seg.decline_problems(FOUND, DECLINES) == []
    first = DECLINES[0]
    moved = (EpisodeSpec(id=first.id, start=first.start, end="2011-01"), *DECLINES[1:])
    assert seg.decline_problems(FOUND, moved) == [
        f"listed {first.id} {first.start}..2011-01 is not recomputed",
        f"recomputed {first.id} {first.start}..{first.end} is not listed",
    ]
    assert seg.decline_problems(FOUND, DECLINES[1:]) == [
        f"recomputed {first.id} {first.start}..{first.end} is not listed"
    ]


# 62. The gate changes only for reserve configurations ---------------------------------------------

TIMING_RULES = {
    "qqq": [
        ("ci_high.cvar_5pct_relative_improvement", ">", 0.0),
        ("comparison.geometric_annual_return", ">=", -0.010),
    ],
    "us_market": [
        ("ci_low.cvar_5pct_relative_improvement", ">", 0.0),
        ("comparison.max_drawdown", ">=", 0.05),
        ("comparison.geometric_annual_return", ">=", -0.010),
        ("cost_25.geometric_annual_return", ">=", -0.010),
        ("comparison.cvar_5pct_relative_improvement", ">=", 0.10),
    ],
}
PORTFOLIO_RULES = {
    "qqq": (
        ("qqq_long_history_development",),
        [
            ("comparison.geometric_annual_return", ">=", -0.010),
            ("portfolio.median_ratio", ">", 0.0),
            ("portfolio.rebalance_return", ">=", 0.0),
        ],
    ),
    "us_market": (
        ("us_market_1929_2010",),
        [
            ("comparison.geometric_annual_return", ">=", -0.010),
            ("cost_25.geometric_annual_return", ">=", -0.010),
            ("portfolio.segment_count", ">=", 4),
            ("portfolio.median_ratio", ">=", 0.10),
            ("portfolio.positive_share", ">=", 0.75),
            ("portfolio.rebalance_return", ">=", 0.0),
        ],
    ),
}


def test_the_timing_rules_are_word_for_word_and_the_ruler_judges_named_windows_only() -> None:
    gate = tr.load_gate(GATE_FILE)

    assert gate.owner_decision == "owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler"
    assert (gate.max_configs_per_trial, gate.confidence) == (8, 0.95)
    for market, expected in TIMING_RULES.items():
        assert [(r.metric, r.op, r.value) for r in gate.markets[market].rules] == expected
    for market, (windows, rules) in PORTFOLIO_RULES.items():
        ruler = gate.markets[market].portfolio_rules
        assert ruler is not None
        assert ruler.windows == windows
        assert [(r.metric, r.op, r.value) for r in ruler.rules] == rules
    us = gate.markets["us_market"]
    assert us.reserve_question_types == {"treasury_10y": "us_market_bond_trial"}
    assert us.portfolio_rules_for("us_market_1962_2010") is None
    assert gate.markets["qqq"].reserve_question_types is None
    assert tr.gate_problems(gate, LOADED) == []


def test_q_is_the_probe_share_rule_on_whole_counts() -> None:
    # positive_share >= 0.75 holds exactly when ceil(0.75 n) declines are cushioned.
    for count in range(4, 61):
        for positive in range(count + 1):
            assert (positive / count >= 0.75) is (positive >= int(np.ceil(0.75 * count)))


# 63. Frozen things stay frozen -------------------------------------------------------------------

PLAN_SHA256 = {
    "T0001": "da29eff1e01029da47d61e2b8d75ae8f7f1e57db7c52bf4ba70c5c67fe2d4a7c",
    "T0002": "137fe6c223c727b83a0ffcf49e65bd88ec8a61be0c114589b729dde5c498a461",
    "T0003": "e9449136096bebcb5fdb77cc158658b2d57a4eb5f89deda9f0d6e475950a34b0",
    "T0004": "b401f2e2108e1bcdd1711a8ea02163390a236dd519b59c93ead9fdec29027917",
    "T0005": "12c15f8c947ade12965eb3ec2d95d056b063099ab9ccd96309d44075cf3067be",
    "T0006": "d2b52987afc529c51bebdca45fce25c0c23191a83c0375fcc3f2b12d625db39d",
    "T0007": "1f3ec2783bc4954c7f84e2a7ae5214424378cd779de31ba35cea082b2954732e",
}
GATE_COPY_SHA256 = {
    "trial_gate_v1.yaml": "b17d97a9f9358f34c404cf84b28e031c64c2f87296b080bb7cd0c53e94fa3b0d",
    "trial_gate_v2.yaml": "d9de5cc21bd1f02085eab72b8d87bb8715db2f62dbe4d7f1105dce44cad7380f",
}


def test_the_plans_the_gate_copies_and_the_reserves_are_unchanged() -> None:
    for trial_id, digest in PLAN_SHA256.items():
        path = PROJECT_ROOT / f"research/_trials/{trial_id}.yaml"
        assert lg.chain_digest(path.read_bytes()) == digest, trial_id
    for name, digest in GATE_COPY_SHA256.items():
        path = PROBES / name
        assert lg.chain_digest(path.read_bytes()) == digest, name
        for entry in tr.load_gate(path).markets.values():
            assert entry.portfolio_rules is None and entry.decline_episodes is None
    assert RESERVES == ("cash", "treasury_10y")


def _old_verdict(first: dict[str, bool]) -> str:
    """primary_verdict before the ruler, on the first successful run of each market."""
    if any(not holds for holds in first.values()):
        return "fail"
    return "pass" if all(market in first for market in tr.MARKETS) else "incomplete"


def test_records_written_before_the_ruler_keep_their_verdict() -> None:
    choices: list[bool | None] = [True, False, None]
    for us, qqq in itertools.product(choices, choices):
        records = [
            {
                "trial_id": "T0002",
                "config": "p",
                "market": market,
                "status": "ok",
                "gate_holds": holds,
            }
            for market, holds in (("us_market", us), ("qqq", qqq))
            if holds is not None
        ]
        first = {r["market"]: bool(r["gate_holds"]) for r in records}
        assert tr.primary_verdict(records, "T0002", "p") == _old_verdict(first)
