"""Acceptance tests for TRADING-2565-105: returns after the 200-day pullback signal on closes.

Requirement: knowledge qqq-options/auto-loop/period7/entry-timing/entry-timing-v0-returns-
requirements-2026-11-01.md (Baker 2026-11-01, owner_decision:TRADING-2565:2026-11-01:
entry_timing_v0_returns). Synthetic data only; no snapshot, no network.
"""

from __future__ import annotations

import ast
import json
import math
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import (
    BenchData,
    from_daily_frame,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_returns as er
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(ROOT_DIR, pt.DATA_TRIAL)
SIGNAL_SNAPSHOT = PLAN.data.signal_snapshot_id
QQQ_SNAPSHOT = PLAN.data.snapshot_id
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "entry_timing_returns.py"
PROVENANCE = {"commit": "c" * 40, "branch": "main", "code_modified": False}
GROWTH = 1.0005
DIP = 0.9


# Constants -----------------------------------------------------------------------------------


def test_the_run_prints_returns_under_bakers_decision() -> None:
    assert er.RUN_ID == "TRADING-2565-105"
    assert er.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-01:entry_timing_v0_returns"
    assert er.VARIANT == "v0_close" == ec.VARIANT
    assert er.HORIZONS == (20, 60)
    assert er.STOP == 0.08
    assert er.RESULT_JSON == "entry_timing_returns.json"
    assert er.RESULT_MD == "entry_timing_returns.md"


def test_the_module_reads_only_through_the_counts_module() -> None:
    text = MODULE.read_text(encoding="utf-8")
    imported: set[str] = set()
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.update(f"{node.module}.{alias.name}" for alias in node.names)
    allowed = (
        "__future__.",
        "json",
        "math",
        "collections.",
        "dataclasses.",
        "datetime.",
        "pathlib.",
        "typing.",
        "typer",
        "ai_trading_system.config.PROJECT_ROOT",
        "ai_trading_system.core.provenance.git_provenance",
        "ai_trading_system.research_bench.trial",
        "ai_trading_system.research_bench.data.",
        "ai_trading_system.research_bench.settings.",
        "ai_trading_system.research_data.qqq_long_history",
        "ai_trading_system.research_probes.entry_timing_counts",
    )
    for name in imported:
        assert name.startswith(allowed), name
    assert "ai_trading_system.research_probes.entry_timing_counts" in imported
    banned = ("ma200_forward", "window_reads", "ledger.jsonl", ".unlink(", "urllib", "requests")
    for word in banned:
        assert word not in text
    assert text.count(".write_text(") == 2


# One entry -----------------------------------------------------------------------------------


def test_the_plain_return_is_the_hth_close_over_the_entry_close() -> None:
    close = [10.0, 11.0, 12.0, 13.0, 14.0]
    found = er.outcome(close, 1, 3)
    assert found is not None
    assert found.plain == pytest.approx(14.0 / 11.0 - 1.0)
    assert found.stop == found.plain
    assert found.stop_day is None


@pytest.mark.parametrize(("entry", "horizon"), [(1, 4), (4, 1), (0, 5)])
def test_an_entry_without_its_hth_close_has_no_outcome(entry: int, horizon: int) -> None:
    assert er.outcome([1.0, 1.0, 1.0, 1.0, 1.0], entry, horizon) is None


def test_a_close_at_exactly_ninety_two_percent_stops() -> None:
    found = er.outcome([1.0, 0.95, 0.92, 1.2, 1.3], 0, 4)
    assert found is not None
    assert found.stop_day == 2
    assert found.stop == pytest.approx(0.92 - 1.0)
    assert found.plain == pytest.approx(0.3)


def test_a_close_just_above_ninety_two_percent_does_not_stop() -> None:
    found = er.outcome([1.0, 0.9200001, 0.9200001, 1.1], 0, 3)
    assert found is not None
    assert found.stop_day is None
    assert found.stop == found.plain == pytest.approx(0.1)


def test_the_stop_exits_at_the_first_breaching_close_even_past_eight_percent() -> None:
    found = er.outcome([2.0, 2.0, 1.7, 1.5, 2.4], 0, 4)
    assert found is not None
    assert found.stop_day == 2
    assert found.stop == pytest.approx(1.7 / 2.0 - 1.0)
    assert found.plain == pytest.approx(0.2)


def test_the_stop_can_fire_on_the_last_day_and_not_after() -> None:
    on_last = er.outcome([1.0, 1.0, 1.0, 0.5], 0, 3)
    after = er.outcome([1.0, 1.0, 1.0, 1.0, 0.5], 0, 3)
    assert on_last is not None and after is not None
    assert on_last.stop_day == 3 and on_last.stop == on_last.plain == pytest.approx(-0.5)
    assert after.stop_day is None and after.plain == 0.0


def test_the_entry_close_itself_never_stops() -> None:
    found = er.outcome([1.0, 0.5, 0.6, 0.7], 1, 2)
    assert found is not None
    assert found.stop_day is None
    assert found.plain == pytest.approx(0.4)


# Baselines -----------------------------------------------------------------------------------


def _day(n: int) -> date:
    return date.fromordinal(date(2001, 1, 1).toordinal() + n)


def test_a_baseline_averages_every_day_in_range_with_an_outcome() -> None:
    dates = [_day(n) for n in range(5)]
    outcomes = [
        er.Outcome(0.1, 0.1, None),
        None,
        er.Outcome(-0.2, -0.1, 1),
        er.Outcome(0.0, 0.0, None),
        er.Outcome(0.4, -0.09, 2),
    ]
    found = er.baseline(dates, outcomes, _day(0), _day(3))
    assert found["n"] == 3
    assert found["plain_mean"] == pytest.approx((0.1 - 0.2 + 0.0) / 3)
    assert found["stop_mean"] == pytest.approx((0.1 - 0.1 + 0.0) / 3)
    assert found["plain_up_share"] == pytest.approx(1 / 3)
    assert found["stop_up_share"] == pytest.approx(1 / 3)


def test_an_empty_baseline_has_no_means() -> None:
    found = er.baseline([_day(0)], [None], _day(0), _day(0))
    assert found == {
        "n": 0,
        "plain_mean": None,
        "stop_mean": None,
        "plain_up_share": None,
        "stop_up_share": None,
    }


# One market on a known path -------------------------------------------------------------------


def _path(days: int, dips: dict[int, float]) -> list[float]:
    level, close = 1.0, []
    for p in range(days):
        level *= GROWTH
        close.append(level * dips.get(p, 1.0))
    return close


def _series(close: list[float], start: str) -> ec.Closes:
    days = pd.bdate_range(start, periods=len(close))
    return ec.Closes(tuple(d.date() for d in days), tuple(close), 0)


def _expected_year(series: ec.Closes, first: int, year: int, h: int) -> tuple[int, float, float]:
    found = [
        er.outcome(series.close, d, h)
        for d in range(first, len(series.close))
        if series.dates[d].year == year
    ]
    kept = [o for o in found if o is not None]
    return (
        len(kept),
        math.fsum(o.plain for o in kept) / len(kept),
        math.fsum(o.stop for o in kept) / len(kept),
    )


def test_a_signal_row_carries_its_returns_and_its_years_average() -> None:
    close = _path(700, {300: DIP})
    close[420:] = [c * 0.85 for c in close[420:]]  # a lasting drop later in the same year
    series = _series(close, "2001-01-01")
    found = er.market_returns(series, "qqq", series.dates[0], series.dates[-1])
    assert [s["entry"] for s in found["signals"]] == [series.dates[303].isoformat()]
    assert series.dates[303].year == series.dates[420].year
    signal = found["signals"][0]
    assert signal["touch"] == series.dates[300].isoformat()
    assert signal["year"] == series.dates[303].year
    assert signal["range"] == "1999_2010"
    for h in er.HORIZONS:
        cell = signal[f"h{h}"]
        one = er.outcome(series.close, 303, h)
        assert one is not None
        assert cell["plain"] == pytest.approx(one.plain)
        assert cell["stop"] == pytest.approx(one.stop)
        assert cell["stop_day"] == one.stop_day
        n, plain, stop = _expected_year(series, 0, signal["year"], h)
        assert (cell["year_n"], cell["year_plain"], cell["year_stop"]) == (
            n,
            pytest.approx(plain),
            pytest.approx(stop),
        )
        assert cell["year_stop"] < cell["year_plain"]
    assert set(signal) == {"touch", "entry", "year", "range", "h20", "h60"}
    assert set(signal["h20"]) == {"plain", "stop", "stop_day", "year_n", "year_plain", "year_stop"}


def test_the_year_baseline_skips_days_before_the_window_and_past_the_end() -> None:
    close = _path(560, {300: DIP})
    series = _series(close, "2001-01-01")
    start = series.dates[250]
    found = er.market_returns(series, "qqq", start, series.dates[-1])
    signal = found["signals"][0]
    first = 250
    year = signal["year"]
    for h in er.HORIZONS:
        n, plain, stop = _expected_year(series, first, year, h)
        assert signal[f"h{h}"]["year_n"] == n
        assert signal[f"h{h}"]["year_plain"] == pytest.approx(plain)
        days = [
            d
            for d in range(first, len(close))
            if series.dates[d].year == year and d + h < len(close)
        ]
        assert n == len(days)


def test_a_signal_without_sixty_more_closes_has_no_sixty_day_cell() -> None:
    close = _path(340, {300: DIP})
    series = _series(close, "2001-01-01")
    found = er.market_returns(series, "qqq", series.dates[0], series.dates[-1])
    signal = found["signals"][0]
    assert signal["h20"] is not None
    assert signal["h60"] is None
    summary = found["ranges"][0]
    assert summary["signals"] == 1
    assert summary["h60"]["settled"] == 0
    assert summary["h60"]["plain_mean"] is None
    assert summary["h60"]["year_stop_mean"] is None
    assert summary["h20"]["settled"] == 1


def test_uncounted_and_failed_touches_are_not_signals() -> None:
    close = _path(700, {300: DIP, 400: DIP, 402: 0.8})
    series = _series(close, "2001-01-01")
    touches = ec.scan(series.close, 0)
    counted = [t.entry for t in touches if t.counted]
    found = er.market_returns(series, "qqq", series.dates[0], series.dates[-1])
    assert [s["entry"] for s in found["signals"]] == [
        series.dates[e].isoformat() for e in counted if e is not None
    ]
    assert any(t.outcome == "failed" for t in touches)


def test_a_signal_whose_average_is_falling_is_not_listed() -> None:
    close = [1.0 / GROWTH**i for i in range(600)]
    close[399] = close[398] * 3.0
    close[401] = close[402] = close[403] = close[399]
    series = _series(close, "2001-01-01")
    touches = ec.scan(series.close, 0)
    assert [(t.outcome, t.counted) for t in touches][:1] == [("signal", False)]
    found = er.market_returns(series, "qqq", series.dates[0], series.dates[-1])
    assert found["signals"] == []
    assert found["ranges"][0]["signals"] == 0
    assert found["ranges"][0]["h60"]["any_day_n"] == 600 - 60


def test_a_us_signal_takes_the_era_of_its_touch_and_the_year_of_its_entry() -> None:
    close = _path(400, {})
    index = pd.bdate_range(end="1961-12-29", periods=320)
    later = pd.bdate_range("1962-01-01", periods=80)
    days = tuple(d.date() for d in index.append(later))
    close[250] *= DIP  # a signal entered in 1961
    close[318] *= DIP  # touch 1961-12-28, entry three closes later in 1962
    series = ec.Closes(days, tuple(close), 0)
    found = er.market_returns(series, "us_market", date(1929, 1, 1), date(2010, 12, 31))
    assert [s["year"] for s in found["signals"]] == [1961, 1962]
    for h in er.HORIZONS:
        n, plain, _ = _expected_year(series, 0, 1962, h)
        cell = found["signals"][1][f"h{h}"]
        assert (cell["year_n"], cell["year_plain"]) == (n, pytest.approx(plain))
        assert cell["year_n"] != found["signals"][0][f"h{h}"]["year_n"]
    signal = found["signals"][1]
    assert signal["touch"] == "1961-12-28"
    assert signal["entry"] == days[321].isoformat()
    assert signal["year"] == 1962
    assert signal["range"] == "1929_1961"
    ranges = {r["range"]: r for r in found["ranges"]}
    assert list(ranges) == ["1929_2010", "1929_1961", "1962_1989", "1990_2010"]
    assert ranges["1929_1961"]["signals"] == 2 and ranges["1962_1989"]["signals"] == 0
    assert ranges["1929_1961"]["start"] == "1929-01-01"
    assert ranges["1990_2010"]["end"] == "2010-12-31"


def test_a_range_summary_counts_and_averages_its_signals() -> None:
    dips = {300: DIP, 420: DIP, 450: 0.85, 600: DIP}
    close = _path(900, dips)
    series = _series(close, "2001-01-01")
    found = er.market_returns(series, "qqq", series.dates[0], series.dates[-1])
    cells = {h: [s[f"h{h}"] for s in found["signals"]] for h in er.HORIZONS}
    summary = found["ranges"][0]
    assert summary["signals"] == len(found["signals"]) >= 2
    for h in er.HORIZONS:
        s, c = summary[f"h{h}"], cells[h]
        assert s["settled"] == len(c)
        assert s["plain_mean"] == pytest.approx(math.fsum(x["plain"] for x in c) / len(c))
        assert s["stop_mean"] == pytest.approx(math.fsum(x["stop"] for x in c) / len(c))
        assert s["year_plain_mean"] == pytest.approx(math.fsum(x["year_plain"] for x in c) / len(c))
        assert s["year_stop_mean"] == pytest.approx(math.fsum(x["year_stop"] for x in c) / len(c))
        assert s["plain_up"] == sum(x["plain"] > 0 for x in c)
        assert s["stop_up"] == sum(x["stop"] > 0 for x in c)
        assert s["plain_beats_year"] == sum(x["plain"] > x["year_plain"] for x in c)
        assert s["stop_beats_year"] == sum(x["stop"] > x["year_stop"] for x in c)
        assert s["stopped"] == sum(x["stop_day"] is not None for x in c)
        every = [er.outcome(series.close, d, h) for d in range(len(close))]
        kept = [o for o in every if o is not None]
        assert s["any_day_n"] == len(kept) == len(close) - h
        assert s["any_day_plain_mean"] == pytest.approx(
            math.fsum(o.plain for o in kept) / len(kept)
        )
        assert s["any_day_stop_mean"] == pytest.approx(math.fsum(o.stop for o in kept) / len(kept))
        assert s["any_day_plain_up_share"] == pytest.approx(
            sum(o.plain > 0 for o in kept) / len(kept)
        )
        assert s["any_day_stop_up_share"] == pytest.approx(
            sum(o.stop > 0 for o in kept) / len(kept)
        )
    assert summary["h60"]["stopped"] >= 1


# Both rulers ---------------------------------------------------------------------------------


def _pos(index: pd.DatetimeIndex, day: str) -> int:
    position = index.get_loc(pd.Timestamp(day))
    assert isinstance(position, int)
    return position


def _closes_to_returns(index: pd.DatetimeIndex, dips: list[str], crash: str) -> np.ndarray:
    level = GROWTH ** np.arange(len(index), dtype=float)
    for day in dips:
        level[_pos(index, day)] *= DIP
    level[_pos(index, crash) :] *= 0.85
    returns = np.empty(len(index))
    returns[0] = 0.0
    returns[1:] = level[1:] / level[:-1] - 1.0
    return returns


US_DIPS = ["1940-06-03", "1961-12-20", "1975-03-03", "1999-05-03", "2010-12-29"]
QQQ_DIPS = ["2002-06-03", "2006-09-05", "2010-12-01"]


def _us_data() -> BenchData:
    index = pd.bdate_range("1926-07-01", "2012-12-31", name="date")
    returns = _closes_to_returns(index, US_DIPS, "1975-03-20")
    factors = pd.DataFrame({"Mkt-RF": returns, "SMB": 0.0, "HML": 0.0, "RF": 0.0}, index=index)
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id=SIGNAL_SNAPSHOT)
    return gp.cut(with_signal_sources(data, {}, {}, snapshot_id=SIGNAL_SNAPSHOT))


def _qqq_data() -> BenchData:
    index = pd.bdate_range("1998-01-02", "2012-12-31", name="date")
    returns = _closes_to_returns(index, QQQ_DIPS, "2006-09-20")
    returns[index <= pd.Timestamp("1999-03-10")] = np.nan
    level = 100.0 * GROWTH ** np.arange(len(index), dtype=float)
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
        index=index,
    )
    data = from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": QQQ_SNAPSHOT})
    return gp.cut(with_signal_sources(data, {}, {}, snapshot_id=SIGNAL_SNAPSHOT))


DATA = {"us_market": _us_data(), "qqq": _qqq_data()}


def _holed(day: str) -> dict[str, BenchData]:
    data = DATA["us_market"]
    returns = data.returns.copy()
    returns.loc[pd.Timestamp(day), "qqq"] = np.nan
    return {
        **DATA,
        "us_market": BenchData(returns=returns, inputs=data.inputs, source=dict(data.source)),
    }


@pytest.fixture(scope="module")
def probe() -> dict[str, Any]:
    return er.run_probe(DATA, LOADED, GATE, provenance=PROVENANCE)


def test_the_probe_runs_both_rulers_after_a_passing_check(probe: dict[str, Any]) -> None:
    assert probe["run_id"] == "TRADING-2565-105"
    assert probe["owner_decision"] == er.OWNER_DECISION
    assert probe["variant"] == "v0_close"
    assert probe["rule"] == {
        "sma_days": 200,
        "slope_lag": 20,
        "confirm_days": 3,
        "cooldown": 20,
        "hold_days": 60,
        "horizons": [20, 60],
        "stop": 0.08,
    }
    assert probe["provenance"] == PROVENANCE
    assert probe["check"] == ec.check_inputs(DATA, LOADED, GATE)
    assert probe["complete"] is True
    assert list(probe["markets"]) == ["qqq", "us_market"]
    assert set(probe) == {
        "run_id",
        "owner_decision",
        "variant",
        "rule",
        "provenance",
        "check",
        "markets",
        "complete",
    }


def test_each_ruler_matches_the_market_returns_on_its_window(probe: dict[str, Any]) -> None:
    for market in ("qqq", "us_market"):
        row = probe["check"]["markets"][market]
        start, end = date.fromisoformat(row["start"]), date.fromisoformat(row["end"])
        series = ec.closes(DATA[market].returns["qqq"], end)
        expected = er.market_returns(series, market, start, end)
        found = probe["markets"][market]
        assert found["window_id"] == row["window_id"]
        assert (found["start"], found["end"]) == (row["start"], row["end"])
        assert found["signals"] == expected["signals"]
        assert found["ranges"] == expected["ranges"]
        assert set(found) == {"window_id", "start", "end", "signals", "ranges"}


def test_the_counted_signals_are_the_counts_modules(probe: dict[str, Any]) -> None:
    us = probe["markets"]["us_market"]
    counted = {r["range"]: r["counted"] for r in probe["check"]["markets"]["us_market"]["ranges"]}
    assert {r["range"]: r["signals"] for r in us["ranges"]} == counted
    qqq = probe["markets"]["qqq"]
    assert [s["entry"] for s in qqq["signals"]] == probe["check"]["markets"]["qqq"]["entries"]
    assert all(s["entry"] <= "2010-12-31" for s in us["signals"])
    assert us["signals"][-1]["entry"] < "2010-12-29"


def test_the_crash_after_a_signal_triggers_the_stop(probe: dict[str, Any]) -> None:
    us = {s["touch"]: s for s in probe["markets"]["us_market"]["signals"]}
    crashed = us["1975-03-03"]["h20"]
    assert crashed["stop_day"] is not None
    assert crashed["stop"] < -0.08
    qqq = {s["touch"]: s for s in probe["markets"]["qqq"]["signals"]}
    assert qqq["2006-09-05"]["h60"]["stop_day"] is not None
    assert qqq["2002-06-03"]["h60"]["stop_day"] is None


def test_a_failed_check_computes_no_returns() -> None:
    probe = er.run_probe(_holed("1950-06-01"), LOADED, GATE, provenance=PROVENANCE)
    assert probe["complete"] is False
    assert probe["markets"] == {}
    assert probe["check"]["ok"] is False


# Markdown and screen ------------------------------------------------------------------------


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{100.0 * value:+.2f}%"


def _share(value: float | None) -> str:
    return "—" if value is None else f"{100.0 * value:.1f}%"


def test_the_markdown_lists_every_signal_and_every_summary(probe: dict[str, Any]) -> None:
    text = er.render_markdown(probe)
    lines = text.split("\n")
    assert text.endswith("|\n")
    assert (
        lines[0]
        == "# 200 日线回踩收盘价版：信号后 20/60 天收益对当年任一天入场（TRADING-2565-105）"
    )
    assert lines[1] == ""
    assert lines[2] == er.RULE_TEXT
    assert "8% 止损 = 持有期内第一个收盘 ≤ 入场 × 0.92 就按那天的收盘出场" in er.RULE_TEXT
    assert "1952 年以前有周六交易日" in er.RULE_TEXT
    signal_header = (
        "| 触线日 | 入场日 | 20 天 | 20 天止损 | 当年 20 天 | 当年 20 天止损 "
        "| 60 天 | 60 天止损 | 当年 60 天 | 当年 60 天止损 |"
    )
    summary_header = (
        "| 范围 | 天数 | 信号 | 均值 | 止损均值 | 当年均值 | 当年止损均值 | 上涨 | 止损上涨 "
        "| 赢当年 | 止损赢当年 | 止损触发 | 任一天均值 | 任一天止损均值 | 任一天上涨比例 "
        "| 任一天止损上涨比例 |"
    )
    expected = lines[:3]
    for market, title in (("qqq", "QQQ 1999–2010"), ("us_market", "美股大盘 1929–2010")):
        found = probe["markets"][market]
        expected += ["", f"## {title}", "", signal_header, "|" + "---|" * 10]
        for s in found["signals"]:
            cells = [s["touch"], s["entry"]]
            for h in (20, 60):
                c = s[f"h{h}"]
                if c is None:
                    cells += ["—"] * 4
                    continue
                stop = _pct(c["stop"])
                if c["stop_day"] is not None:
                    stop += f"（第 {c['stop_day']} 天止损）"
                cells += [_pct(c["plain"]), stop, _pct(c["year_plain"]), _pct(c["year_stop"])]
            expected.append("| " + " | ".join(cells) + " |")
        expected += ["", summary_header, "|" + "---|" * 16]
        for r in found["ranges"]:
            for h in (20, 60):
                v, k = r[f"h{h}"], r[f"h{h}"]["settled"]
                cells = [r["range"].replace("_", "–"), str(h), str(k)]
                cells += [_pct(v[key]) for key in ("plain_mean", "stop_mean")]
                cells += [_pct(v[key]) for key in ("year_plain_mean", "year_stop_mean")]
                cells += [
                    f"{v[key]}/{k}"
                    for key in (
                        "plain_up",
                        "stop_up",
                        "plain_beats_year",
                        "stop_beats_year",
                        "stopped",
                    )
                ]
                cells += [_pct(v["any_day_plain_mean"]), _pct(v["any_day_stop_mean"])]
                cells += [_share(v["any_day_plain_up_share"]), _share(v["any_day_stop_up_share"])]
                expected.append("| " + " | ".join(cells) + " |")
    assert text == "\n".join(expected) + "\n"
    assert "天止损）" in text
    assert "| — | — | — | — |" in text


def test_the_screen_line_gives_the_sixty_day_means(probe: dict[str, Any], tmp_path: Path) -> None:
    q = probe["markets"]["qqq"]["ranges"][0]
    u = probe["markets"]["us_market"]["ranges"][0]
    assert er.summary_line(probe, tmp_path) == (
        f"entry_timing_returns: QQQ 1999–2010 信号 {q['signals']}，60 天均值 "
        f"{_pct(q['h60']['plain_mean'])}（止损 {_pct(q['h60']['stop_mean'])}），当年任一天 "
        f"{_pct(q['h60']['year_plain_mean'])}（止损 {_pct(q['h60']['year_stop_mean'])}）；"
        f"美股大盘 1929–2010 信号 {u['signals']}，60 天均值 {_pct(u['h60']['plain_mean'])}"
        f"（止损 {_pct(u['h60']['stop_mean'])}），当年任一天 {_pct(u['h60']['year_plain_mean'])}"
        f"（止损 {_pct(u['h60']['year_stop_mean'])}）；结果目录 {tmp_path}"
    )


# CLI -----------------------------------------------------------------------------------------


@pytest.fixture
def loaded_cli(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    roots: list[Path] = []

    def load_data(root: Path) -> Any:
        roots.append(root)
        return DATA, GATE, LOADED, ROOT_DIR

    monkeypatch.setattr(er, "load_data", load_data)
    monkeypatch.setattr(er, "git_provenance", lambda root: PROVENANCE)
    return roots


def test_run_writes_the_json_and_the_table(
    loaded_cli: list[Path], probe: dict[str, Any], tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(er.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [tmp_path]
    text = out.joinpath("entry_timing_returns.json").read_text(encoding="utf-8")
    assert text == json.dumps(probe, indent=2, ensure_ascii=False) + "\n"
    table = out.joinpath("entry_timing_returns.md").read_text(encoding="utf-8")
    assert table == er.render_markdown(probe)
    assert sorted(p.name for p in out.iterdir()) == [
        "entry_timing_returns.json",
        "entry_timing_returns.md",
    ]
    assert result.output == er.summary_line(probe, out) + "\n"


@pytest.mark.parametrize("existing", ["entry_timing_returns.json", "entry_timing_returns.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, existing: str
) -> None:
    def load_data(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(er, "load_data", load_data)
    tmp_path.joinpath(existing).write_text("x", encoding="utf-8")
    result = CliRunner().invoke(er.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_timing_returns: 已有结果，拒绝覆盖。\n"
    assert [p.name for p in tmp_path.iterdir()] == [existing]


def test_a_load_failure_writes_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def load_data(root: Path) -> Any:
        raise tr.TrialError("snapshot is not checked\nsecond line")

    monkeypatch.setattr(er, "load_data", load_data)
    out = tmp_path / "out"
    result = CliRunner().invoke(er.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == "entry_timing_returns: 数据载入失败：snapshot is not checked\n"
    assert not out.exists()


def test_a_failed_check_writes_only_the_json(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    holed = _holed("1950-06-01")
    monkeypatch.setattr(er, "load_data", lambda root: (holed, GATE, LOADED, ROOT_DIR))
    monkeypatch.setattr(er, "git_provenance", lambda root: PROVENANCE)
    result = CliRunner().invoke(er.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_timing_returns: 输入检查 FAIL，没有算收益\n"
    assert [p.name for p in tmp_path.iterdir()] == ["entry_timing_returns.json"]
    written = json.loads(tmp_path.joinpath("entry_timing_returns.json").read_text("utf-8"))
    assert written["complete"] is False and written["markets"] == {}


def test_the_load_goes_through_the_counts_module(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[Path] = []
    sentinel: Any = object()

    def load(root: Path) -> Any:
        calls.append(root)
        return sentinel

    monkeypatch.setattr(ec, "load_data", load)
    assert er.load_data(tmp_path) is sentinel
    assert calls == [tmp_path]
