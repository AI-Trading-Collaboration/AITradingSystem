"""Invariants of the read-only slow-bear and long-bull segment tables (TRADING-2565-36).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2565:2026-10-21:slow_bear_readonly``). All data is synthetic.

48. A strategy that holds the market itself loses nothing inside a segment: at exposure 1 every
    listed segment has D = 0 and the strategy's return inside it is the segment's depth from the
    fixed rule; a constant exposure 0.5 also has D = 0 (the matched benchmark, not buy-and-hold).
49. D moves with returns: on hand-built simulations D is the strategy's compounded return inside
    the segment's months minus the benchmark's; a return inside the segment moves it, one outside
    does not; long bulls annualise over the segment's calendar span.
50. Read only: the lists never touch the fingerprint, the metrics, the events or a gate rule (no
    rule may name a segment metric); a run without lists has no section; a listed segment with no
    return day keeps an empty row, stays out of the summaries and never fails the run.
51. The lists are the rule's: the gate lists exactly the slow-bear probe's segments
    (TRADING-2565-34) for exactly the development windows of each market; recomputing on the
    same data agrees and any difference is named; malformed lists are refused.
52. Frozen things stay frozen: the plans T0001–T0007 and the probes' gate copies are byte for byte
    what they were, the copies still load, and the probe runs the bench's segment code.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Sequence
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import segments as seg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData, from_daily_frame
from ai_trading_system.research_bench.fixed_strategy import fixed_exposure
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR
from ai_trading_system.research_bench.prereg import rule_sections
from ai_trading_system.research_bench.run import RunSpec, run_strategy
from ai_trading_system.research_bench.settings import EpisodeSpec, load_settings, resolve_window
from ai_trading_system.research_bench.simulate import Rebalance, Simulation
from ai_trading_system.research_probes import slow_bear_check as sb

LOADED = load_settings()
DEV = resolve_window(LOADED, "qqq_direction", "development")
GATE_FILE = PROJECT_ROOT / "research/_trials/prereg.yaml"
PROBES = PROJECT_ROOT / "src/ai_trading_system/research_probes"
# Daily rebalancing keeps a constant exposure exactly constant, as in invariant 1.
DAILY = Rebalance(schedule="daily")

# The slow-bear probe's segments (TRADING-2565-34, main 30e50b4f, run on 2026-10-21 02:07-02:41
# JST): a slow bear runs from the month after the high to the trough. Fast drops are not listed.
US_1929_SLOW = [
    ("slow_bear_192908", "1929-09", "1932-06"),
    ("slow_bear_193208", "1932-09", "1933-02"),
    ("slow_bear_193702", "1937-03", "1938-03"),
    ("slow_bear_193909", "1939-10", "1942-04"),
    ("slow_bear_194605", "1946-06", "1947-05"),
    ("slow_bear_196112", "1962-01", "1962-06"),
    ("slow_bear_196811", "1968-12", "1970-06"),
    ("slow_bear_197212", "1973-01", "1974-09"),
    ("slow_bear_200003", "2000-04", "2002-09"),
    ("slow_bear_200710", "2007-11", "2009-02"),
]
US_1929_LONG = [
    ("long_bull_193303", "1933-03", "1937-02"),
    ("long_bull_194205", "1942-05", "1946-05"),
    ("long_bull_194706", "1947-06", "1961-12"),
    ("long_bull_196207", "1962-07", "1968-11"),
    ("long_bull_197410", "1974-10", "1987-08"),
    ("long_bull_198712", "1987-12", "2000-03"),
    ("long_bull_200210", "2002-10", "2007-10"),
]
PROBE_LISTS = {
    "us_market": {
        "us_market_1929_2010": (US_1929_SLOW, US_1929_LONG),
        "us_market_1962_2010": (US_1929_SLOW[6:], US_1929_LONG[3:]),
    },
    "qqq": {
        "qqq_long_history_development": (
            [
                ("slow_bear_200003", "2000-04", "2002-09"),
                ("slow_bear_200710", "2007-11", "2009-02"),
            ],
            [("long_bull_200210", "2002-10", "2007-10")],
        )
    },
}
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
# Monthly moves of the planted market: a 12-month slow bear, a 3-month fast drop, long bulls.
PLANTED = [
    ("1997-01", "2000-03", 0.015),
    ("2000-04", "2001-03", -0.04),
    ("2001-04", "2002-12", 0.03),
    ("2003-01", "2003-03", -0.09),
    ("2003-04", "2011-06", 0.012),
]


def _monthly_path(moves: Sequence[tuple[str, str, float]], seed: int = 11) -> pd.Series:
    index = pd.bdate_range("1997-01-02", "2011-06-30", name="date")
    months = index.to_period("M")
    out = pd.Series(0.0, index=index)
    for first, last, move in moves:
        inside = (months >= pd.Period(first, freq="M")) & (months <= pd.Period(last, freq="M"))
        for month in pd.unique(months[inside]):
            days = months == month
            out[days] = (1.0 + move) ** (1.0 / int(days.sum())) - 1.0
    noise = np.random.default_rng(seed).normal(0.0, 0.002, len(index))
    return out + noise - noise.mean()


def _data(returns: pd.Series) -> BenchData:
    level = 1000.0 * np.cumprod(1.0 + returns.to_numpy())
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": returns.to_numpy(),
            "etf_reference_return": returns.to_numpy(),
            "cash_return": 0.0001,
            "lev2_return": 2 * returns.to_numpy(),
            "lev3_return": 3 * returns.to_numpy(),
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=returns.index,
    )
    return from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "synthetic"})


DATA = _data(_monthly_path(PLANTED))
FOUND = seg.segments(seg.window_returns(DATA.returns["qqq"], DEV))


def _specs(found: Sequence[seg.Segment], kind: str) -> tuple[EpisodeSpec, ...]:
    return tuple(EpisodeSpec(id=s.id, start=s.start, end=s.end) for s in found if s.kind == kind)


SLOW = _specs(FOUND, "slow_bear")
LONG = _specs(FOUND, "long_bull")


def _spec(exposure: float) -> RunSpec:
    return RunSpec(
        question_type="qqq_direction",
        strategy_id=f"fixed_{exposure:g}",
        params={"exposure": exposure},
        window_role="development",
        rebalance=DAILY,
    )


def _run(exposure: float, **lists: Any) -> Any:
    return run_strategy(DATA, fixed_exposure, _spec(exposure), LOADED, **lists)


def _sim(returns: pd.Series, exposure: pd.Series) -> Simulation:
    daily = pd.DataFrame({"return": returns, "exposure": exposure})
    return Simulation(daily=daily, start=pd.Timestamp(returns.index[0]) - pd.offsets.BDay(1))


# 48. A strategy that holds the market loses nothing inside a segment ------------------------------


def test_the_planted_market_has_every_kind_of_segment() -> None:
    assert [(s.id, s.start, s.end, s.months) for s in FOUND] == [
        ("slow_bear_200003", "2000-04", "2001-03", 12),
        ("fast_drop_200212", "2003-01", "2003-03", 3),
        ("long_bull_200304", "2003-04", "2010-12", 93),
    ]


@pytest.mark.parametrize("exposure", [1.0, 0.5])
def test_holding_the_market_has_no_difference_inside_any_segment(exposure: float) -> None:
    result = _run(exposure, slow_bears=SLOW, long_bulls=LONG)
    section = result.segments
    depth = {s.id: s.change for s in FOUND}

    assert [row["id"] for row in section["slow_bear"]] == [s.id for s in SLOW]
    assert [row["id"] for row in section["long_bull"]] == [s.id for s in LONG]
    for row in section["slow_bear"] + section["long_bull"]:
        assert row["difference"] == pytest.approx(0.0, abs=1e-12)
        assert row["average_exposure"] == pytest.approx(exposure, abs=1e-12)
        if exposure == 1.0:
            assert row["strategy_return"] == pytest.approx(depth[row["id"]], rel=1e-12)
            assert row["benchmark_return"] == pytest.approx(depth[row["id"]], rel=1e-12)
        else:
            assert abs(row["strategy_return"] - depth[row["id"]]) > 0.01
    for row in section["long_bull"]:
        assert row["annual_difference"] == pytest.approx(0.0, abs=1e-12)
    summary = section["slow_bear_summary"]
    assert summary["count"] == len(SLOW) and summary["median_difference"] == pytest.approx(0.0)
    assert section["long_bull_summary"]["count"] == len(LONG)


def test_a_list_shifted_by_a_month_is_not_the_depth() -> None:
    slow = SLOW[0]
    shifted = EpisodeSpec(id=slow.id, start=str(pd.Period(slow.start, freq="M") - 1), end=slow.end)
    row = _run(1.0, slow_bears=(shifted,)).segments["slow_bear"][0]
    depth = next(s.change for s in FOUND if s.id == slow.id)

    assert row["months"] == 13
    assert abs(row["strategy_return"] - depth) > 1e-4


# 49. D moves with returns -------------------------------------------------------------------------


def test_the_difference_is_the_compounded_return_inside_the_months() -> None:
    index = pd.bdate_range("2001-01-01", "2001-05-31", name="date")
    own = pd.Series(0.001, index=index)
    reference = pd.Series(0.0005, index=index)
    exposure = pd.Series(np.where(index.month == 2, 0.0, 1.0), index=index)
    spec = EpisodeSpec(id="slow_bear_200101", start="2001-02", end="2001-03")
    inside = (index.month >= 2) & (index.month <= 3)

    def difference(a: pd.Series, b: pd.Series) -> float:
        section = seg.segment_section(_sim(a, exposure), _sim(b, exposure), (spec,), ())
        assert section is not None
        row = section["slow_bear"][0]
        assert row["days"] == int(inside.sum())
        assert row["average_exposure"] == pytest.approx(float(exposure[inside].mean()))
        return float(row["difference"])

    hand = float(np.prod(1 + own[inside]) - np.prod(1 + reference[inside]))
    assert difference(own, reference) == pytest.approx(hand, rel=1e-12)

    up = own.copy()
    up.iloc[np.flatnonzero(inside)[3]] += 0.02
    moved = difference(up, reference)
    assert moved - hand == pytest.approx(0.02 * np.prod(1 + own[inside]) / 1.001, rel=1e-9)

    outside = own.copy()
    outside.iloc[0] += 0.05
    outside.iloc[-1] -= 0.05
    assert difference(outside, reference) == pytest.approx(hand, rel=1e-12)

    worse = reference.copy()
    worse.iloc[np.flatnonzero(inside)[0]] += 0.01
    assert difference(own, worse) < hand


def test_long_bulls_annualise_over_the_calendar_span() -> None:
    index = pd.bdate_range("2004-01-01", "2008-12-31", name="date")
    own = pd.Series(0.0004, index=index)
    reference = pd.Series(0.0003, index=index)
    exposure = pd.Series(0.8, index=index)
    spec = EpisodeSpec(id="long_bull_200402", start="2004-02", end="2008-06")
    section = seg.segment_section(_sim(own, exposure), _sim(reference, exposure), (), (spec,))
    assert section is not None
    row = section["long_bull"][0]
    inside = (index >= "2004-02-01") & (index <= "2008-06-30")
    before = index[~inside & (index < "2004-02-01")][-1]
    years = (index[inside][-1] - before).days / DAYS_PER_YEAR

    def annual(r: pd.Series) -> float:
        return float(np.prod(1 + r[inside]) ** (1 / years) - 1)

    assert row["months"] == 53
    assert row["strategy_annual"] == pytest.approx(annual(own), rel=1e-12)
    assert row["benchmark_annual"] == pytest.approx(annual(reference), rel=1e-12)
    assert row["annual_difference"] == pytest.approx(annual(own) - annual(reference), rel=1e-12)
    summary = section["long_bull_summary"]
    assert summary["count"] == 1
    assert summary["mean_annual_difference"] == pytest.approx(row["annual_difference"])
    assert summary["worst_annual_difference"] == pytest.approx(row["annual_difference"])
    assert section["slow_bear"] == [] and section["slow_bear_summary"]["count"] == 0


# 50. Read only ------------------------------------------------------------------------------------


def test_the_lists_never_touch_the_fingerprint_the_metrics_or_the_events() -> None:
    plain = _run(1.0)
    listed = _run(1.0, slow_bears=SLOW, long_bulls=LONG)

    assert plain.segments is None and listed.segments is not None
    a, b = plain.to_dict(), listed.to_dict()
    assert a.pop("segments") is None and b.pop("segments") == listed.segments
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    assert plain.fingerprint == listed.fingerprint


def test_no_gate_rule_can_read_a_segment() -> None:
    assert "segments" not in rule_sections(LOADED.settings)
    raw = yaml.safe_load(GATE_FILE.read_text(encoding="utf-8"))
    raw["markets"]["us_market"]["rules"].append(
        {"metric": "segments.slow_bear_summary", "op": ">", "value": 0.0}
    )
    with pytest.raises(ValueError, match="metric must be"):
        tr.TrialGate.model_validate(raw)


def test_a_listed_segment_with_no_return_day_never_fails_the_run() -> None:
    late = EpisodeSpec(id="slow_bear_201112", start="2012-01", end="2012-09")
    result = _run(1.0, slow_bears=(*SLOW, late), long_bulls=LONG)
    rows = result.segments["slow_bear"]

    assert rows[-1] == {
        "id": "slow_bear_201112",
        "kind": "slow_bear",
        "start": "2012-01",
        "end": "2012-09",
        "months": 9,
        "days": 0,
        "strategy_return": None,
        "benchmark_return": None,
        "difference": None,
        "average_exposure": None,
    }
    assert result.segments["slow_bear_summary"]["count"] == len(SLOW)
    assert result.fingerprint == _run(1.0).fingerprint


# 51. The lists are the rule's ---------------------------------------------------------------------


def test_the_gate_lists_the_probe_segments_of_each_development_window() -> None:
    gate = tr.load_gate(GATE_FILE)
    for market, windows in PROBE_LISTS.items():
        entry = gate.markets[market]
        developed = {
            resolve_window(LOADED, question_type, "development").window_id
            for question_type in entry.question_types()
        }
        assert set(entry.segment_windows()) == set(windows) == developed
        for window_id, (slow, long) in windows.items():
            listed = entry.segment_lists(window_id)
            assert [(e.id, e.start, e.end) for e in listed[0]] == slow
            assert [(e.id, e.start, e.end) for e in listed[1]] == long
    assert tr.gate_problems(gate, LOADED) == []


def test_recomputing_the_lists_agrees_and_names_any_difference() -> None:
    assert seg.list_problems(FOUND, SLOW, LONG) == []
    moved = (EpisodeSpec(id=SLOW[0].id, start=SLOW[0].start, end="2001-04"),)
    assert seg.list_problems(FOUND, moved, LONG) == [
        f"listed {SLOW[0].id} {SLOW[0].start}..2001-04 is not recomputed",
        f"recomputed {SLOW[0].id} {SLOW[0].start}..{SLOW[0].end} is not listed",
    ]
    assert seg.list_problems(FOUND, SLOW, LONG[1:]) == [
        f"recomputed {LONG[0].id} {LONG[0].start}..{LONG[0].end} is not listed"
    ]


def _with_lists(slow: list[dict[str, str]], window: str = "us_market_1929_2010") -> dict[str, Any]:
    raw = copy.deepcopy(yaml.safe_load(GATE_FILE.read_text(encoding="utf-8")))
    raw["markets"]["us_market"]["slow_bear_episodes"] = {window: slow}
    return raw


@pytest.mark.parametrize(
    "slow",
    [
        [{"id": "slow_bear_192908", "start": "1932-06", "end": "1929-09"}],
        [
            {"id": "slow_bear_192908", "start": "1929-09", "end": "1932-06"},
            {"id": "slow_bear_192908", "start": "1937-03", "end": "1938-03"},
        ],
        [
            {"id": "slow_bear_192908", "start": "1929-09", "end": "1932-06"},
            {"id": "slow_bear_193208", "start": "1932-06", "end": "1933-02"},
        ],
        [
            {"id": "slow_bear_193702", "start": "1937-03", "end": "1938-03"},
            {"id": "slow_bear_192908", "start": "1929-09", "end": "1937-03"},
        ],
        [{"id": "slow_bear_196112", "start": "1962-01", "end": "1962-07"}],
        [{"id": "Slow bear", "start": "1929-09", "end": "1932-06"}],
        [{"id": "slow_bear_192908", "start": "1929-9", "end": "1932-06"}],
    ],
)
def test_malformed_lists_are_refused(slow: list[dict[str, str]]) -> None:
    with pytest.raises(ValueError):
        tr.TrialGate.model_validate(_with_lists(slow))


def test_lists_for_a_window_the_market_never_runs_are_a_gate_problem() -> None:
    slow = [{"id": "slow_bear_200003", "start": "2000-04", "end": "2002-09"}]
    gate = tr.TrialGate.model_validate(_with_lists(slow, "qqq_long_history_development"))
    assert tr.gate_problems(gate, LOADED) == [
        "us_market: segments listed for qqq_long_history_development, not a development window here"
    ]


# 52. Frozen things stay frozen --------------------------------------------------------------------


def test_the_plans_and_the_probe_gate_copies_are_unchanged() -> None:
    for trial_id, digest in PLAN_SHA256.items():
        path = PROJECT_ROOT / f"research/_trials/{trial_id}.yaml"
        assert lg.chain_digest(path.read_bytes()) == digest, trial_id
    for name, digest in GATE_COPY_SHA256.items():
        path = PROBES / name
        assert lg.chain_digest(path.read_bytes()) == digest, name
        gate = tr.load_gate(path)
        for entry in gate.markets.values():
            assert entry.slow_bear_episodes is None and entry.long_bull_episodes is None


def test_the_probe_runs_the_bench_segment_code() -> None:
    for name in (
        "Segment",
        "window_returns",
        "monthly_level",
        "drop_segments",
        "long_bull_segments",
        "segments",
        "segment_return",
        "annual_return",
        "segment_rows",
        "slow_bear_summary",
        "long_bull_summary",
    ):
        assert getattr(sb, name) is getattr(seg, name), name
    assert (sb.DROP, sb.SLOW_MONTHS, sb.LONG_BULL_MONTHS) == (0.20, 6, 36)
    assert (seg.DROP, seg.SLOW_MONTHS, seg.LONG_BULL_MONTHS) == (0.20, 6, 36)
