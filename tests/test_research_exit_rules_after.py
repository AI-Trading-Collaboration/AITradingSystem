"""Acceptance tests for TRADING-2565-113/-115: falls in the 20 closes after each QQQ exit signal.

Requirement: knowledge qqq-options/auto-loop/period7/exit-family/exit-family-after-signal-
requirements-2026-11-02.md (owner decision exit_family_after_signal); -115 adds the volatility
columns the review asked for. Synthetic data only; no snapshot, no network.
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
from ai_trading_system.research_bench.data import BenchData, from_daily_frame, with_signal_sources
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_after as xa
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_ndx as xn
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.yaml_loader import safe_load_yaml_path

LOADED = load_settings()
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(lg.research_root(LOADED), pt.DATA_TRIAL)
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "exit_rules_after.py"
REGISTRY = PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
SOURCE = {"snapshot_id": "qlh-20261013-0b0df53cb1a2", "column": "ndx_price", "check_status": "WARN"}
WINDOW = en.registered_window(LOADED)


# Constants and imports ------------------------------------------------------------------------


def test_the_run_is_the_after_signal_table_of_the_exit_family() -> None:
    assert xa.RUN_ID == "TRADING-2565-113"
    assert xa.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-02:exit_family_after_signal"
    assert xa.RULES == xc.RULES == ("X0", "X1", "X2", "X3", "X4", "X5", "X6")
    assert xa.SPANS == ("qqq", "ndx")
    assert xa.VIEWS == ("trades", "days", "clusters")
    assert xa.MATCHED == ("fall_mean", "fall_share10", "scaled_fall_mean", "sigma_mean", "r20_mean")
    assert (xa.AFTER, xa.TAIL, xa.FALL_LINES, xa.BANDS) == (20, 0.05, (0.05, 0.10), 5)
    assert (xa.RESULT_JSON, xa.RESULT_MD) == ("exit_rules_after.json", "exit_rules_after.md")
    assert xa.SPAN_TEXT == {"qqq": "QQQ 1999–2010（开发窗口，切到 2010-12-31）", "ndx": en.TITLE}


def test_the_note_says_what_is_measured_and_what_is_not() -> None:
    for words in (
        "只印不判",
        "k 都照第 0 步写死",
        "信号日是判断日",
        "没跌算 0",
        "21 个收盘",
        "每簇只取第一天",
        "同 20 日波动档（五档）",
        "不算错过的反弹",
        "平均 σ20",
        "按 σ 缩放的同档对照",
        "最大跌幅 ÷ σ20",
        "价目表已经印过（从卖出日算，差一天）",
    ):
        assert words in xa.NOTE, words


def test_the_module_reads_only_qqq_and_its_index_through_the_family() -> None:
    text = MODULE.read_text(encoding="utf-8")
    tree = ast.parse(text)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.update(f"{node.module}.{alias.name}" for alias in node.names)
    allowed = (
        "__future__.",
        "json",
        "math",
        "collections.",
        "datetime.",
        "pathlib.",
        "typing.",
        "typer",
        "ai_trading_system.research_bench.trial",
        "ai_trading_system.research_bench.data.BenchData",
        "ai_trading_system.research_bench.settings.LoadedSettings",
        "ai_trading_system.research_data.qqq_long_history",
        "ai_trading_system.research_probes.entry_timing_counts",
        "ai_trading_system.research_probes.entry_timing_ndx",
        "ai_trading_system.research_probes.exit_rules_counts",
        "ai_trading_system.research_probes.exit_rules_ndx",
        "ai_trading_system.research_probes.exit_rules_returns",
        "ai_trading_system.research_probes.policy_table",
    )
    for name in imported:
        assert name.startswith(allowed), name
    for required in (
        "entry_timing_ndx",
        "exit_rules_counts",
        "exit_rules_ndx",
        "exit_rules_returns",
    ):
        assert f"ai_trading_system.research_probes.{required}" in imported
    banned = (
        "us_market",
        "French",
        "Mkt-RF",
        "ma200_forward",
        "window_reads",
        "ledger",
        "pt.load_data",
        "policy_table.load",
        ".unlink(",
        "urllib",
        "requests",
        "safe_load_yaml",
        "load_daily",
        "def exit_for",
        "def prepare",
        "def check_step0",
        "def check_inputs",
        "def prices",
        "def tail_mean",
        "def rank",
        "H_MATCH",
        "numpy",
        "pandas",
    )
    for word in banned:
        assert word not in text, word
    assert text.count(".write_text(") == 2
    for call in ("xc.load_data(", "en.load_data(", "xr.check_step0(", "xn.check_inputs("):
        assert call in text, call


def test_the_registry_names_the_third_reader_and_keeps_the_window() -> None:
    text = REGISTRY.read_text(encoding="utf-8")
    block = text[text.index("# Baker's print-only read of the 200-day pullback rule") :]
    note = block[: block.index("  ndx_price_1986_1999:")]
    for words in (
        "TRADING-2565-113",
        "owner_decision:TRADING-2565:2026-11-02:exit_family_after_signal",
        "research_probes/exit_rules_after.py",
        "20 closes after each exit signal",
    ):
        assert words in " ".join(note.replace("#", " ").split()), words
    window = safe_load_yaml_path(REGISTRY)["windows"]["ndx_price_1986_1999"]
    assert str(window["start"]) == "1986-01-02"
    assert str(window["end"]) == "1999-03-09"
    assert window["role"] == "owner_print_only"
    assert window["question_types"] == []
    assert window["read_policy"] == "once_per_owner_decision"


# Small pieces --------------------------------------------------------------------------------


def _ramp(values: list[float]) -> list[float]:
    return values + [values[-1]] * (xa.AFTER + 1 - len(values))


def test_the_fall_is_the_lowest_of_the_next_twenty_closes() -> None:
    close = _ramp([100.0, 103.0, 96.0, 110.0, 99.0, 104.0])
    one = xa.after(close, 0)
    assert one["fall"] == pytest.approx(-0.04)
    assert one["drawdown"] == pytest.approx(99.0 / 110.0 - 1.0)
    assert one["next"] == pytest.approx(0.03)
    assert one["r20"] == pytest.approx(0.04)


def test_no_fall_counts_as_zero_and_the_start_counts_for_the_drawdown() -> None:
    close = _ramp([100.0, 101.0, 102.0, 108.0])
    one = xa.after(close, 0)
    assert one["fall"] == 0.0
    assert one["drawdown"] == 0.0
    falling = _ramp([100.0, 90.0, 95.0])
    assert xa.after(falling, 0)["drawdown"] == pytest.approx(-0.10)


def test_the_window_is_exactly_twenty_closes() -> None:
    close = [100.0] * (xa.AFTER + 1) + [50.0]
    assert xa.after(close, 0)["fall"] == 0.0
    assert xa.after(close, 1)["fall"] == pytest.approx(-0.5)


def test_the_statistics_of_a_set_of_falls() -> None:
    falls = [-0.12, -0.05, 0.0, -0.02, -0.10, 0.0, -0.30, -0.049, 0.0, -0.01]
    rows = [
        {"fall": f, "drawdown": f - 0.01, "next": 0.001 * i, "r20": 0.02 - 0.005 * i}
        for i, f in enumerate(falls)
    ]
    got = xa.stats(rows)
    assert got["n"] == 10
    assert got["fall_mean"] == pytest.approx(sum(falls) / 10)
    assert got["fall_median"] == sorted(falls)[4]
    assert got["fall_p05"] == -0.30
    assert got["fall_cvar05"] == -0.30
    assert got["fall_share05"] == pytest.approx(0.4)
    assert got["fall_share10"] == pytest.approx(0.3)
    assert got["drawdown_mean"] == pytest.approx(sum(falls) / 10 - 0.01)
    assert got["drawdown_p05"] == pytest.approx(-0.31)
    assert got["next_mean"] == pytest.approx(0.0045)
    assert got["r20_mean"] == pytest.approx(0.02 - 0.0225)
    assert got["r20_up"] == pytest.approx(0.4)


def test_an_empty_set_has_no_statistics() -> None:
    got = xa.stats([])
    assert got["n"] == 0
    assert all(value is None for key, value in got.items() if key != "n")
    assert list(got) == list(xa.stats([{"fall": 0.0, "drawdown": 0.0, "next": 0.0, "r20": 0.0}]))


def test_clusters_keep_the_first_day_after_a_gap_of_twenty() -> None:
    days = [50, 5, 6, 30, 49, 80, 5, 99, 119]
    assert xa.first_days(days) == [5, 30, 80, 119]
    assert len(xa.first_days(days)) == xc.clusters(days)
    assert xa.first_days([]) == []


def test_a_mean_and_the_volatility_texts() -> None:
    assert xa.mean([]) is None
    assert xa.mean([0.1, 0.2, 0.6]) == pytest.approx(0.3)
    assert (xa.sigma_text(0.01814), xa.sigma_text(None)) == ("1.81%", "—")
    ratios = (xa.ratio_text(-2.954), xa.ratio_text(0.5), xa.ratio_text(None))
    assert ratios == ("-2.95", "+0.50", "—")


def test_volatility_bands_are_nearest_rank_quintiles() -> None:
    values = [float(v) for v in range(1, 11)]
    edges = xa.band_edges(values)
    assert edges == [2.0, 4.0, 6.0, 8.0]
    assert [xa.band(v, edges) for v in (1.0, 2.0, 3.0, 8.0, 8.5, 10.0)] == [0, 0, 1, 3, 4, 4]


# Synthetic QQQ ruler --------------------------------------------------------------------------


def _pos(index: pd.DatetimeIndex, day: str) -> int:
    position = index.get_loc(pd.Timestamp(day))
    assert isinstance(position, int)
    return position


def _qqq_data() -> BenchData:
    index = pd.bdate_range("1998-01-02", "2012-12-31", name="date")
    rng = np.random.default_rng(11)
    level = 100.0 * np.cumprod(1.0003 * (1.0 + rng.normal(0.0, 0.015, len(index))))
    for first, last, depth in (
        ("2000-04-03", "2000-05-24", 0.7),
        ("2008-09-15", "2008-11-20", 0.6),
    ):
        a, b = _pos(index, first), _pos(index, last)
        level[a : b + 1] *= np.linspace(1.0, depth, b - a + 1)
        level[b + 1 :] *= depth
    returns = np.empty(len(index))
    returns[0] = 0.0
    returns[1:] = level[1:] / level[:-1] - 1.0
    returns[index <= pd.Timestamp("1999-03-10")] = np.nan
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
    data = from_daily_frame(
        daily, qqq_series="actual", source={"snapshot_id": PLAN.data.snapshot_id}
    )
    return gp.cut(with_signal_sources(data, {}, {}, snapshot_id=PLAN.data.signal_snapshot_id))


QQQ = _qqq_data()
STEP0 = xc.check_inputs(QQQ, LOADED, GATE)
Q_SERIES = ec.closes(QQQ.returns["qqq"], date(2010, 12, 31))
Q_PREPARED = xc.prepare(Q_SERIES.dates, Q_SERIES.close)
Q_FIRST = next(p for p, day in enumerate(Q_SERIES.dates) if day >= date(1999, 3, 10))
Q_ENTRIES = xc.entry_range(Q_PREPARED, Q_FIRST)
Q_K = xc.scale(Q_PREPARED, Q_ENTRIES)
assert Q_K is not None
Q_EXITS = {r: {e: xc.exit_for(r, Q_PREPARED, e, Q_K) for e in Q_ENTRIES} for r in xc.RULES}


# Synthetic Nasdaq-100 levels ------------------------------------------------------------------

INDEX = pd.bdate_range("1986-01-02", "1999-03-09", name="date")


def _ndx_levels() -> list[tuple[date, float]]:
    rng = np.random.default_rng(5)
    level = 100.0 * np.cumprod(1.0006 * (1.0 + rng.normal(0.0, 0.011, len(INDEX))))
    for first, last, depth in (
        ("1987-10-12", "1987-10-23", 0.7),
        ("1990-07-16", "1990-10-12", 0.8),
    ):
        a, b = _pos(INDEX, first), _pos(INDEX, last)
        level[a : b + 1] *= np.linspace(1.0, depth, b - a + 1)
        level[b + 1 :] *= depth
    for day in ("1988-04-01", "1995-12-25"):
        level[_pos(INDEX, day)] = level[_pos(INDEX, day) - 1]
    return [(day.date(), float(value)) for day, value in zip(INDEX, level, strict=True)]


LEVELS = _ndx_levels()
N_SERIES, N_FLAT = en.prices(LEVELS)
N_PREPARED = xc.prepare(N_SERIES.dates, N_SERIES.close)
N_ENTRIES = xc.entry_range(N_PREPARED, 0)
# Under same_data, xr.STEP0_K is the synthetic QQQ k, the factor the module must use here.
N_EXITS = {r: {e: xc.exit_for(r, N_PREPARED, e, Q_K) for e in N_ENTRIES} for r in xc.RULES}
N_MOVE = en.largest_move(N_SERIES)
assert N_MOVE is not None


@pytest.fixture
def same_data(monkeypatch: pytest.MonkeyPatch) -> None:
    a = STEP0["entry_set_a"]
    monkeypatch.setattr(xr, "STEP0_TRADES", a["trades"])
    monkeypatch.setattr(xr, "STEP0_FIRST", a["first"])
    monkeypatch.setattr(xr, "STEP0_LAST", a["last"])
    monkeypatch.setattr(xr, "STEP0_K", STEP0["scale"]["k"])
    monkeypatch.setattr(xr, "H_MATCH", {r: STEP0["exits"][r]["h_match"] for r in xc.RULES})
    assert N_MOVE is not None
    monkeypatch.setattr(xn, "NDX_CLOSES", len(N_SERIES.dates))
    monkeypatch.setattr(xn, "NDX_FLAT_DATES", tuple(day.isoformat() for day in N_FLAT))
    monkeypatch.setattr(xn, "NDX_MOVE_DATE", N_MOVE["date"])
    monkeypatch.setattr(xn, "NDX_MOVE", round(N_MOVE["return"], 4))


@pytest.fixture
def table(same_data: None) -> dict[str, Any]:
    return xa.run_table(QQQ, LOADED, GATE, LEVELS, SOURCE, WINDOW)


def test_the_synthetic_data_are_not_the_real_ones() -> None:
    assert STEP0["entry_set_a"]["trades"] != xr.STEP0_TRADES
    assert len(N_SERIES.dates) != xn.NDX_CLOSES
    assert len(N_ENTRIES) >= xn.MIN_TRADES


# Checks --------------------------------------------------------------------------------------


def test_the_real_constants_stop_the_table_before_any_fall() -> None:
    result = xa.run_table(QQQ, LOADED, GATE, LEVELS, SOURCE, WINDOW)
    assert result["complete"] is False
    assert result["check"]["qqq"]["ok"] is False
    assert result["check"]["ndx"]["ok"] is False
    assert "spans" not in result
    assert list(result) == ["run_id", "owner_decision", "source", "rule", "check", "complete"]


def test_each_span_s_check_alone_stops_the_table(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(xn, "NDX_CLOSES", len(N_SERIES.dates) + 1)
    one = xa.run_table(QQQ, LOADED, GATE, LEVELS, SOURCE, WINDOW)
    assert (one["check"]["qqq"]["ok"], one["check"]["ndx"]["ok"], one["complete"]) == (
        True,
        False,
        False,
    )
    monkeypatch.setattr(xn, "NDX_CLOSES", len(N_SERIES.dates))
    monkeypatch.setattr(xr, "STEP0_K", STEP0["scale"]["k"] * 1.001)
    two = xa.run_table(QQQ, LOADED, GATE, LEVELS, SOURCE, WINDOW)
    assert (two["check"]["qqq"]["ok"], two["check"]["ndx"]["ok"], two["complete"]) == (
        False,
        True,
        False,
    )


def test_the_checks_are_the_family_s_own(table: dict[str, Any]) -> None:
    window = pt.market_window(LOADED, GATE, xc.MARKET)
    assert window.end is not None
    q = table["check"]["qqq"]
    assert (q["window_id"], q["start"], q["end"]) == (
        window.window_id,
        window.start.isoformat(),
        window.end.isoformat(),
    )
    assert {k: v for k, v in q.items() if k not in ("window_id", "start", "end")} == (
        xr.check_step0(Q_SERIES, Q_ENTRIES, Q_K, Q_EXITS)
    )
    assert table["check"]["ndx"] == xn.check_inputs(N_SERIES, N_FLAT, WINDOW, N_ENTRIES)
    assert table["source"] == {"qqq": {"snapshot_id": PLAN.data.snapshot_id}, "ndx": SOURCE}
    assert table["rule"] == {
        "k": STEP0["scale"]["k"],
        "after": 20,
        "tail": 0.05,
        "fall_lines": [0.05, 0.10],
        "bands": 5,
        "cluster_gap": xc.CLUSTER_GAP,
        "signal_day": "decision",
        "sell": "next_close",
    }
    assert table["complete"] is True
    assert list(table["spans"]) == ["qqq", "ndx"]


def test_the_ndx_exits_use_the_frozen_stop_factor(
    same_data: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[float] = []
    real = xc.exit_for

    def spy(rule: str, data: xc.Prepared, e: int, k: float) -> xc.Exit:
        calls.append(k)
        return real(rule, data, e, k)

    monkeypatch.setattr(xc, "exit_for", spy)
    xa.run_table(QQQ, LOADED, GATE, LEVELS, SOURCE, WINDOW)
    assert set(calls) == {Q_K} and Q_K == STEP0["scale"]["k"]
    assert len(calls) == 7 * (len(Q_ENTRIES) + len(N_ENTRIES))


# Every number recomputed --------------------------------------------------------------------


def _after(close: list[float], t: int) -> dict[str, float]:
    window = np.asarray(close[t : t + 21])
    peaks = np.maximum.accumulate(window)
    return {
        "fall": float(min(0.0, window[1:].min() / window[0] - 1.0)),
        "drawdown": float((window / peaks - 1.0).min()),
        "next": float(window[1] / window[0] - 1.0),
        "r20": float(window[20] / window[0] - 1.0),
    }


def _stats(rows: list[dict[str, float]]) -> dict[str, Any]:
    if not rows:
        return {"n": 0}
    falls = sorted(r["fall"] for r in rows)
    drawdowns = sorted(r["drawdown"] for r in rows)
    n = len(rows)
    tail = max(math.ceil(0.05 * n), 1)
    return {
        "n": n,
        "fall_mean": float(np.mean(falls)),
        "fall_median": falls[max(math.ceil(0.5 * n) - 1, 0)],
        "fall_p05": falls[max(math.ceil(0.05 * n) - 1, 0)],
        "fall_cvar05": float(np.mean(falls[:tail])),
        "fall_share05": float(np.mean([f <= -0.05 for f in falls])),
        "fall_share10": float(np.mean([f <= -0.10 for f in falls])),
        "drawdown_mean": float(np.mean(drawdowns)),
        "drawdown_p05": drawdowns[max(math.ceil(0.05 * n) - 1, 0)],
        "next_mean": float(np.mean([r["next"] for r in rows])),
        "r20_mean": float(np.mean([r["r20"] for r in rows])),
        "r20_up": float(np.mean([r["r20"] > 0 for r in rows])),
    }


def _same(got: dict[str, Any], want: dict[str, Any]) -> None:
    assert got["n"] == want["n"]
    for key, value in want.items():
        assert got[key] == pytest.approx(value, rel=1e-9, abs=1e-12), key


SPAN_INPUTS = {
    "qqq": (Q_SERIES, Q_PREPARED, Q_ENTRIES, Q_EXITS),
    "ndx": (N_SERIES, N_PREPARED, N_ENTRIES, N_EXITS),
}


def _sigmas(prepared: xc.Prepared, days: list[int]) -> dict[int, float]:
    return {t: s for t in days if (s := prepared.sigma[t]) is not None}


def _same_sigma(
    got: dict[str, Any], days: list[int], sig: dict[int, float], rows: dict[int, dict[str, float]]
) -> None:
    assert got["sigma_mean"] == pytest.approx(float(np.mean([sig[t] for t in days])), rel=1e-9)
    ratio = float(np.mean([rows[t]["fall"] / sig[t] for t in days]))
    assert got["fall_per_sigma_mean"] == pytest.approx(ratio, rel=1e-9, abs=1e-12)


def _base(span: str) -> tuple[list[float], list[int], dict[int, dict[str, float]]]:
    series, prepared, entries, _ = SPAN_INPUTS[span]
    close = list(series.close)
    days = [t for t in range(entries[0], len(close) - 20) if prepared.sigma[t] is not None]
    return close, days, {t: _after(close, t) for t in days}


@pytest.mark.parametrize("span", ["qqq", "ndx"])
def test_the_any_day_base_and_the_bands(table: dict[str, Any], span: str) -> None:
    series, prepared, _, _ = SPAN_INPUTS[span]
    _, days, rows = _base(span)
    got = table["spans"][span]
    assert got["first_day"] == series.dates[days[0]].isoformat()
    assert got["last_day"] == series.dates[days[-1]].isoformat()
    assert got["last_day"] == series.dates[len(series.close) - 21].isoformat()
    _same(got["any_day"], _stats(list(rows.values())))
    sig = _sigmas(prepared, days)
    _same_sigma(got["any_day"], days, sig, rows)
    sigmas = sorted(sig.values())
    edges = [sigmas[max(math.ceil(q * len(sigmas)) - 1, 0)] for q in (0.2, 0.4, 0.6, 0.8)]
    assert got["band_edges"] == edges
    assert len(got["bands"]) == 5
    for b, one in enumerate(got["bands"]):
        members = [t for t in days if sum(sig[t] > e for e in edges) == b]
        _same(one, _stats([rows[t] for t in members]))
        _same_sigma(one, members, sig, rows)
        assert list(one)[-2:] == ["sigma_mean", "fall_per_sigma_mean"]
    assert sum(one["n"] for one in got["bands"]) == len(days)


@pytest.mark.parametrize("span", ["qqq", "ndx"])
@pytest.mark.parametrize("rule", xc.RULES)
def test_each_rule_s_three_views(table: dict[str, Any], span: str, rule: str) -> None:
    series, prepared, entries, exits = SPAN_INPUTS[span]
    _, days, rows = _base(span)
    edges = table["spans"][span]["band_edges"]
    sig = _sigmas(prepared, days)
    band_of = {t: sum(sig[t] > e for e in edges) for t in days}
    band_rows = {b: [rows[t] for t in days if band_of[t] == b] for b in range(5)}
    band_days = {b: [t for t in days if band_of[t] == b] for b in range(5)}
    per_sigma = {b: np.mean([rows[t]["fall"] / sig[t] for t in band_days[b]]) for b in range(5)}
    signalled = [x.decision for x in exits[rule].values() if x.reason == "signal"]
    kept = [d for d in signalled if d is not None and d in rows]
    unique = sorted(set(kept))
    firsts = [d for i, d in enumerate(unique) if i == 0 or d - unique[i - 1] >= 20]
    got = table["spans"][span]["rules"][rule]
    assert got["signals"] == len(signalled)
    assert got["beyond_end"] == len(signalled) - len(kept)
    for view, chosen in (("trades", kept), ("days", unique), ("clusters", firsts)):
        one = got["views"][view]
        _same(one, _stats([rows[d] for d in chosen]))
        if chosen:
            want_fall = np.mean(
                [np.mean([r["fall"] for r in band_rows[band_of[d]]]) for d in chosen]
            )
            want_share = np.mean(
                [np.mean([r["fall"] <= -0.10 for r in band_rows[band_of[d]]]) for d in chosen]
            )
            want_scaled = np.mean([sig[d] * per_sigma[band_of[d]] for d in chosen])
            want_sigma = np.mean([np.mean([sig[t] for t in band_days[band_of[d]]]) for d in chosen])
            want_r20 = np.mean([np.mean([r["r20"] for r in band_rows[band_of[d]]]) for d in chosen])
            matched = one["matched"]
            assert list(matched) == list(xa.MATCHED)
            assert matched["fall_mean"] == pytest.approx(float(want_fall), rel=1e-9)
            assert matched["fall_share10"] == pytest.approx(float(want_share), rel=1e-9)
            assert matched["scaled_fall_mean"] == pytest.approx(float(want_scaled), rel=1e-9)
            assert matched["sigma_mean"] == pytest.approx(float(want_sigma), rel=1e-9)
            assert matched["r20_mean"] == pytest.approx(float(want_r20), rel=1e-9, abs=1e-12)
            want_own = float(np.mean([sig[d] for d in chosen]))
            assert one["sigma_mean"] == pytest.approx(want_own, rel=1e-9)
        else:
            assert one["matched"] == dict.fromkeys(xa.MATCHED)
            assert one["sigma_mean"] is None
        assert list(one)[-2:] == ["sigma_mean", "matched"]
    assert [c["date"] for c in got["clusters"]] == [series.dates[d].isoformat() for d in firsts]
    for c, d in zip(got["clusters"], firsts, strict=True):
        assert list(c)[:3] == ["date", "band", "sigma"]
        assert c["band"] == band_of[d]
        assert c["sigma"] == sig[d]
        for key, value in _after(list(series.close), d).items():
            assert c[key] == pytest.approx(value, rel=1e-9, abs=1e-12)


def test_signals_after_the_last_full_window_are_left_out(table: dict[str, Any]) -> None:
    n = len(N_SERIES.close)
    late = {r: dict(N_EXITS[r]) for r in xc.RULES}
    late["X0"][N_ENTRIES[-1]] = xc.Exit(n - 21, n - 20, "signal")
    late["X0"][N_ENTRIES[-2]] = xc.Exit(n - 20, n - 19, "signal")
    late["X0"][N_ENTRIES[-3]] = xc.Exit(n - 2, n - 1, "signal")
    before = table["spans"]["ndx"]["rules"]["X0"]
    after = xa.span_table(N_SERIES, N_PREPARED, N_ENTRIES, late)["rules"]["X0"]
    moved = sum(N_EXITS["X0"][N_ENTRIES[-i]].reason == "signal" for i in (1, 2, 3))
    assert after["signals"] == before["signals"] + 3 - moved
    assert after["beyond_end"] == 2
    assert after["views"]["trades"]["n"] == after["signals"] - 2
    for span in xa.SPANS:
        for rule in xc.RULES:
            one = table["spans"][span]["rules"][rule]
            assert one["views"]["trades"]["n"] + one["beyond_end"] == one["signals"]


def test_the_synthetic_table_has_signals_in_every_view(table: dict[str, Any]) -> None:
    counts = [
        table["spans"][span]["rules"][rule]["views"]["clusters"]["n"]
        for span in xa.SPANS
        for rule in xc.RULES
    ]
    assert sum(n > 0 for n in counts) >= 12


def test_no_nan_reaches_the_table(table: dict[str, Any]) -> None:
    json.dumps(table, allow_nan=False)


# Text ----------------------------------------------------------------------------------------


def test_the_markdown(table: dict[str, Any]) -> None:
    md = xa.render_markdown(table)
    lines = md.split("\n")
    assert lines[0] == "# QQQ 出场信号之后 20 天的最大跌幅（TRADING-2565-113）"
    assert lines[2] == xa.NOTE
    assert md.endswith("。\n") and not md.endswith("\n\n")
    for span in xa.SPANS:
        assert f"## {xa.SPAN_TEXT[span]}" in lines
    rows = [line for line in lines if line.startswith("| X")]
    assert len(rows) == 2 * 7 * 3
    assert all(line.count("|") == 21 for line in rows)
    header = [line for line in lines if line.startswith("| 规则 |")]
    assert len(header) == 2 and header[0].count("|") == 21
    assert header[0].startswith(
        "| 规则 | 看法 | 个数 | 平均最大跌幅 | 任一天 | 同波动档 | "
        "同档按 σ 缩放 | 信号日 σ20 | 同档 σ20 |"
    )
    assert header[0].endswith("| 20 天后涨跌 | 同档 20 天后涨跌 | 20 天后更高 |")
    q = table["spans"]["qqq"]
    one = q["rules"]["X0"]["views"]["clusters"]
    m = one["matched"]
    want = (
        f"| X0 | 簇 | {one['n']} | {xr.pct(one['fall_mean'])} | "
        f"{xr.pct(q['any_day']['fall_mean'])} | {xr.pct(m['fall_mean'])} | "
        f"{xr.pct(m['scaled_fall_mean'])} | {xa.sigma_text(one['sigma_mean'])} | "
        f"{xa.sigma_text(m['sigma_mean'])} | {xr.pct(one['fall_median'])} |"
    )
    tail = f"| {xr.pct(one['r20_mean'])} | {xr.pct(m['r20_mean'])} | {xr.share(one['r20_up'])} |"
    assert any(line.startswith(want) and line.endswith(tail) for line in rows)
    differ = 0
    for span in xa.SPANS:
        got = table["spans"][span]
        for rule in xa.RULES:
            for view, word in (("trades", "每笔"), ("days", "信号日"), ("clusters", "簇")):
                one = got["rules"][rule]["views"][view]
                own, same = (
                    xa.sigma_text(one["sigma_mean"]),
                    xa.sigma_text(one["matched"]["sigma_mean"]),
                )
                differ += own != same
                cells = f"| {xr.pct(one['matched']['scaled_fall_mean'])} | {own} | {same} |"
                assert (
                    sum(
                        f"| {rule} | {word} | {one['n']} |" in line and cells in line
                        for line in rows
                    )
                    >= 1
                ), (span, rule, view)
    assert differ >= 5
    base = q["any_day"]
    assert (
        f"平均 σ20 {xa.sigma_text(base['sigma_mean'])}，"
        f"最大跌幅 ÷ σ20 平均 {xa.ratio_text(base['fall_per_sigma_mean'])}。"
    ) in md
    assert "；平均 σ20：" + "、".join(xa.sigma_text(b["sigma_mean"]) for b in q["bands"]) in md
    assert "；最大跌幅 ÷ σ20 平均：" in md
    for view, word in (("trades", "每笔"), ("days", "信号日"), ("clusters", "簇")):
        assert sum(line.startswith(f"| X3 | {word} |") for line in rows) == 2, view
    assert "20 日波动五档（从低到高）的平均最大跌幅：" in md
    assert "信号数（含 20 天后超出数据末尾、没算的）：X0 " in md


def test_the_summary_line(table: dict[str, Any]) -> None:
    out = Path("o")
    line = xa.summary_line(table, out)
    assert line.startswith("exit_rules_after: 信号后 20 天平均最大跌幅：QQQ 1999–2010")
    assert line.endswith("；结果目录 o")
    q, n = table["spans"]["qqq"], table["spans"]["ndx"]
    assert f"任一天 {xr.pct(q['any_day']['fall_mean'])}，按簇 X0 " in line
    assert f"；{en.TITLE} 任一天 {xr.pct(n['any_day']['fall_mean'])}，按簇 X0 " in line
    last = n["rules"]["X6"]["views"]["clusters"]["fall_mean"]
    assert f"X6 {xr.pct(last)}；结果目录" in line


# Command line --------------------------------------------------------------------------------


@pytest.fixture
def loaded_cli(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    def load_qqq(root: Path) -> Any:
        calls.append(f"qqq {root}")
        return QQQ, GATE, LOADED

    def load_ndx(root: Path) -> Any:
        calls.append(f"ndx {root}")
        return LEVELS, SOURCE, WINDOW

    monkeypatch.setattr(xc, "load_data", load_qqq)
    monkeypatch.setattr(en, "load_data", load_ndx)
    return calls


def test_run_writes_both_files_and_prints_one_line(
    loaded_cli: list[str], table: dict[str, Any], tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(xa.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [f"qqq {tmp_path}", f"ndx {tmp_path}"]
    text = out.joinpath("exit_rules_after.json").read_text(encoding="utf-8")
    assert text == json.dumps(table, indent=2, ensure_ascii=False) + "\n"
    assert out.joinpath("exit_rules_after.md").read_text(encoding="utf-8") == (
        xa.render_markdown(table)
    )
    assert sorted(p.name for p in out.iterdir()) == ["exit_rules_after.json", "exit_rules_after.md"]
    assert result.output == xa.summary_line(table, out) + "\n"


@pytest.mark.parametrize("name", ["exit_rules_after.json", "exit_rules_after.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    def boom(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(xc, "load_data", boom)
    monkeypatch.setattr(en, "load_data", boom)
    tmp_path.joinpath(name).write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(xa.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_after: 已有结果，拒绝覆盖。\n"
    assert [p.name for p in tmp_path.iterdir()] == [name]


@pytest.mark.parametrize(
    "failing", [("qqq", tr.TrialError("snapshot is not checked\nmore")), ("ndx", OSError("gone"))]
)
def test_a_load_failure_writes_nothing(
    loaded_cli: list[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failing: tuple[str, Exception],
) -> None:
    which, error = failing

    def fail(root: Path) -> Any:
        raise error

    monkeypatch.setattr(xc if which == "qqq" else en, "load_data", fail)
    out = tmp_path / "out"
    result = CliRunner().invoke(xa.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    first = str(error).splitlines()[0]
    assert result.output == f"exit_rules_after: 数据载入失败：{first}\n"
    assert not out.exists()


def test_a_failed_check_writes_only_the_json(loaded_cli: list[str], tmp_path: Path) -> None:
    result = CliRunner().invoke(xa.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_after: 输入检查 FAIL，没有算跌幅\n"
    assert [p.name for p in tmp_path.iterdir()] == ["exit_rules_after.json"]
    written = json.loads(tmp_path.joinpath("exit_rules_after.json").read_text(encoding="utf-8"))
    assert written["complete"] is False
    assert "spans" not in written
