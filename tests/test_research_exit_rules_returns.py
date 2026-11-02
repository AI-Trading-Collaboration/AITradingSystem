"""Acceptance tests for TRADING-2565-109: the QQQ exit-rule price list, print only.

Requirement: knowledge qqq-options/auto-loop/period7/exit-family/exit-family-price-list-
requirements-2026-11-02.md (owner decision exit_family_price_list). Synthetic data only; no
snapshot, no network.
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
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
GATE = tr.load_gate(PROJECT_ROOT / "research" / "_trials" / "prereg.yaml")
_, PLAN = tr.load_trial(ROOT_DIR, pt.DATA_TRIAL)
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "exit_rules_returns.py"


# Constants -----------------------------------------------------------------------------------


def test_the_run_is_the_price_list_of_the_exit_family() -> None:
    assert xr.RUN_ID == "TRADING-2565-109"
    assert xr.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-02:exit_family_price_list"
    assert xr.MARKET == "qqq"
    assert xr.RULES == xc.RULES == ("X0", "X1", "X2", "X3", "X4", "X5", "X6")


def test_the_matched_holds_and_step_zero_are_frozen() -> None:
    assert xr.H_MATCH == {"X0": 44, "X1": 69, "X2": 42, "X3": 18, "X4": 89, "X5": 51, "X6": 42}
    assert (xr.STEP0_TRADES, xr.STEP0_FIRST, xr.STEP0_LAST) == (2602, "2000-03-06", "2010-07-12")
    assert xr.STEP0_K == 4.421121529829901
    assert (xr.R60, xr.TAIL, xr.LOSS_LINES, xr.AFTER_DAYS, xr.YEAR_DAYS) == (
        60,
        0.05,
        (0.08, 0.15),
        20,
        252,
    )
    assert (xr.GATE_TAIL, xr.GATE_COST) == (0.10, 0.01)
    assert (xr.RESULT_JSON, xr.RESULT_MD) == ("exit_rules_returns.json", "exit_rules_returns.md")


def test_the_module_reads_only_qqq_through_step_zero() -> None:
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
        "statistics.",
        "collections.",
        "dataclasses.",
        "datetime.",
        "pathlib.",
        "typing.",
        "typer",
        "ai_trading_system.research_bench.trial",
        "ai_trading_system.research_bench.data.",
        "ai_trading_system.research_bench.settings.",
        "ai_trading_system.research_data.qqq_long_history",
        "ai_trading_system.research_probes.entry_timing_counts",
        "ai_trading_system.research_probes.exit_rules_counts",
        "ai_trading_system.research_probes.policy_table",
    )
    for name in imported:
        assert name.startswith(allowed), name
    for required in ("entry_timing_counts", "exit_rules_counts", "policy_table"):
        assert f"ai_trading_system.research_probes.{required}" in imported
    banned = (
        "us_market",
        "French",
        "ma200_forward",
        "window_reads",
        "ledger",
        "pt.load_data",
        ".unlink(",
        "urllib",
        "requests",
        "def exit_for",
        "def prepare",
    )
    for word in banned:
        assert word not in text, word
    assert text.count(".write_text(") == 2
    assert "xc.load_data(" in text


# Small pieces --------------------------------------------------------------------------------


def test_a_trade_return_and_its_deepest_close() -> None:
    close = [100.0, 104.0, 95.0, 102.0, 110.0]
    one = xr.trade(close, 0, 3)
    assert one == xr.Trade(0, 3, 102.0 / 100.0 - 1.0, 95.0 / 100.0 - 1.0)
    assert xr.trade(close, 3, 4).deepest == 0.0
    assert xr.trade([100.0, 101.0, 90.0], 0, 2).deepest == 90.0 / 100.0 - 1.0


@pytest.mark.parametrize(
    ("n", "expected"),
    [(1, [0]), (20, [0]), (21, [0, 1]), (40, [0, 1]), (41, [0, 1, 2])],
)
def test_the_tail_mean_takes_the_worst_ceiling_share(n: int, expected: list[int]) -> None:
    values = [float(v) for v in reversed(range(n))]
    assert xr.tail_mean(values) == sum(float(v) for v in expected) / len(expected)


def test_the_rank_is_the_nearest_rank() -> None:
    assert xr.rank([5.0, 1.0, 3.0, 2.0, 4.0], 0.5) == 3.0
    assert xr.rank([float(v) for v in range(20)], 0.05) == 0.0
    assert xr.rank([float(v) for v in range(21)], 0.05) == 1.0


def _trades(values: list[float], deepest: list[float] | None = None) -> list[xr.Trade]:
    low = deepest if deepest is not None else [min(v, 0.0) for v in values]
    return [xr.Trade(p, p + 1, v, d) for p, (v, d) in enumerate(zip(values, low, strict=True))]


def test_one_side_of_the_price_list() -> None:
    values = [0.1, -0.08, -0.0799, -0.15, -0.2, 0.05, 0.0, 0.02, 0.03, 0.04]
    deepest = [-0.01, -0.1, -0.09, -0.2, -0.25, 0.0, -0.02, -0.03, -0.01, 0.0]
    got = xr.side(_trades(values, deepest))
    assert got == {
        "trades": 10,
        "mean": math.fsum(values) / 10,
        "median": 0.0,
        "worst": -0.2,
        "p05": -0.2,
        "cvar05": -0.2,
        "loss08": 3,
        "loss15": 2,
        "deepest_mean": math.fsum(deepest) / 10,
        "deepest_p05": -0.25,
    }


def test_the_gain_is_positive_when_the_rule_loses_less() -> None:
    assert xr.gain(-0.1, -0.2) == pytest.approx(0.5)
    assert xr.gain(-0.3, -0.2) == pytest.approx(-0.5)
    assert xr.gain(0.02, 0.01) == pytest.approx(1.0)
    assert xr.gain(-0.1, 0.0) is None


def _side(mean: float, cvar05: float, p05: float = -0.1) -> dict[str, float]:
    return {"mean": mean, "cvar05": cvar05, "p05": p05}


def test_the_comparison_and_its_gate_sized_label() -> None:
    got = xr.compare(_side(0.01, -0.18, -0.09), _side(0.02, -0.2, -0.1), 42)
    assert got["mean_difference"] == pytest.approx(-0.01)
    assert got["per_year"] == pytest.approx(-0.01 * 252 / 42)
    assert got["cvar05_gain"] == pytest.approx(0.1)
    assert got["p05_gain"] == pytest.approx(0.1)
    assert got["per_trade_gate_size_like"] is False  # costs 6 points a year
    cheap = xr.compare(_side(0.0199, -0.178), _side(0.02, -0.2), 63)
    assert cheap["per_year"] == pytest.approx(-0.0004)
    assert cheap["per_trade_gate_size_like"] is True
    assert (
        xr.compare(_side(0.0199, -0.182), _side(0.02, -0.2), 63)["per_trade_gate_size_like"]
        is False
    )
    assert (
        xr.compare(_side(0.0174, -0.1), _side(0.02, -0.2), 63)["per_trade_gate_size_like"] is False
    )
    assert (
        xr.compare(_side(0.0176, -0.1), _side(0.02, -0.2), 63)["per_trade_gate_size_like"] is True
    )
    assert xr.compare(_side(0.03, -0.1), _side(0.02, 0.0), 63)["per_trade_gate_size_like"] is False
    assert set(got) == {
        "mean_difference",
        "per_year",
        "cvar05_gain",
        "p05_gain",
        "per_trade_gate_size_like",
    }


def test_after_up_counts_strictly_higher_closes_twenty_sessions_later() -> None:
    close = [float(v) for v in range(30)]
    close[25] = close[5]
    got = xr.after_up(close, [0, 5, 9, 10])
    assert got == {"n": 3, "up": 2, "share": 2 / 3}
    assert xr.after_up(close, [10, 29]) == {"n": 0, "up": 0, "share": None}


def test_the_exposure_counts_open_trades_over_the_hold() -> None:
    trades = [xr.Trade(0, 2, 0.0, 0.0), xr.Trade(1, 5, 0.0, 0.0), xr.Trade(2, 3, 0.0, 0.0)]
    # open on days entry + 1 .. sell: day 1 {0}, 2 {0, 1}, 3 {1, 2}, 4 {1}, 5 {1}, 6 {}
    got = xr.exposure(trades, 2, 1, 6)
    assert got == {"days": 6, "mean": 7 / 12, "p05": 0.0, "p95": 1.0, "max": 1.0}
    assert xr.exposure(trades, 1, 2, 4) == {
        "days": 3,
        "mean": 5 / 3,
        "p05": 1.0,
        "p95": 2.0,
        "max": 2.0,
    }


def test_the_exposure_quantiles_are_nearest_ranks() -> None:
    trades = [xr.Trade(0, k, 0.0, 0.0) for k in range(1, 21)]
    got = xr.exposure(trades, 1, 1, 20)  # open counts 20, 19, ..., 1
    assert (got["p05"], got["p95"], got["max"], got["mean"]) == (1.0, 19.0, 20.0, 10.5)


def test_differences_are_grouped_by_entry_year() -> None:
    dates = [date(2000, 12, 28), date(2000, 12, 29), date(2001, 1, 2), date(2001, 1, 3)]
    rule = _trades([0.1, -0.1, 0.0, 0.05])
    control = _trades([0.05, 0.0, 0.02, 0.0])
    assert xr.by_year(dates, rule, control) == [
        {
            "year": 2000,
            "trades": 2,
            "mean_difference": pytest.approx(-0.025),
            "worst_difference": pytest.approx(-0.1),
        },
        {
            "year": 2001,
            "trades": 2,
            "mean_difference": pytest.approx(0.015),
            "worst_difference": pytest.approx(-0.02),
        },
    ]


# Synthetic QQQ data (the same shape as TRADING-2565-108's tests) ------------------------------

QQQ_DIPS = ["2002-06-03", "2006-09-05", "2010-12-01"]
CRASH = ("2008-09-15", "2008-10-31")
GROWTH = 1.0005
DIP = 0.9


def _pos(index: pd.DatetimeIndex, day: str) -> int:
    position = index.get_loc(pd.Timestamp(day))
    assert isinstance(position, int)
    return position


def _qqq_data(hole: str | None = None) -> BenchData:
    index = pd.bdate_range("1998-01-02", "2012-12-31", name="date")
    level = GROWTH ** np.arange(len(index), dtype=float)
    a, b = _pos(index, CRASH[0]), _pos(index, CRASH[1])
    level[a : b + 1] *= np.linspace(1.0, 0.6, b - a + 1)
    level[b + 1 :] *= 0.6
    for day in QQQ_DIPS:
        level[_pos(index, day)] *= DIP
    returns = np.empty(len(index))
    returns[0] = 0.0
    returns[1:] = level[1:] / level[:-1] - 1.0
    returns[index <= pd.Timestamp("1999-03-10")] = np.nan
    if hole is not None:
        returns[_pos(index, hole)] = np.nan
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


DATA = _qqq_data()
SERIES = ec.closes(DATA.returns["qqq"], date(2010, 12, 31))
PREPARED = xc.prepare(SERIES.dates, SERIES.close)
FIRST_IN = next(p for p, day in enumerate(SERIES.dates) if day >= date(1999, 3, 10))
ENTRIES = xc.entry_range(PREPARED, FIRST_IN)
K = xc.scale(PREPARED, ENTRIES)
assert K is not None
EXITS = {r: {e: xc.exit_for(r, PREPARED, e, K) for e in ENTRIES} for r in xc.RULES}
STEP0 = xc.check_inputs(DATA, LOADED, GATE)
SYNTHETIC_H = {r: STEP0["exits"][r]["h_match"] for r in xc.RULES}


@pytest.fixture
def step0(monkeypatch: pytest.MonkeyPatch) -> None:
    a = STEP0["entry_set_a"]
    monkeypatch.setattr(xr, "STEP0_TRADES", a["trades"])
    monkeypatch.setattr(xr, "STEP0_FIRST", a["first"])
    monkeypatch.setattr(xr, "STEP0_LAST", a["last"])
    monkeypatch.setattr(xr, "STEP0_K", STEP0["scale"]["k"])
    monkeypatch.setattr(xr, "H_MATCH", dict(SYNTHETIC_H))


@pytest.fixture
def table(step0: None) -> dict[str, Any]:
    return xr.run_table(DATA, LOADED, GATE)


def test_the_synthetic_data_differ_from_step_zero() -> None:
    assert STEP0["entry_set_a"]["trades"] != xr.STEP0_TRADES
    assert SYNTHETIC_H != xr.H_MATCH


# Step-zero check -----------------------------------------------------------------------------


def test_the_check_passes_when_the_data_are_step_zero_s(step0: None) -> None:
    check = xr.check_step0(SERIES, ENTRIES, K, EXITS)
    assert check == {
        "missing_days": 0,
        "trades": len(ENTRIES),
        "first": SERIES.dates[ENTRIES[0]].isoformat(),
        "last": SERIES.dates[ENTRIES[-1]].isoformat(),
        "k": K,
        "h_match": SYNTHETIC_H,
        "ok": True,
    }


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("STEP0_TRADES", 2703),
        ("STEP0_FIRST", "2000-03-07"),
        ("STEP0_LAST", "2010-07-16"),
        ("STEP0_K", float(K) * (1.0 + 1e-9)),
    ],
)
def test_any_step_zero_mismatch_fails_the_check(
    step0: None, monkeypatch: pytest.MonkeyPatch, name: str, value: Any
) -> None:
    monkeypatch.setattr(xr, name, value)
    assert xr.check_step0(SERIES, ENTRIES, K, EXITS)["ok"] is False


def test_a_different_matched_hold_fails_the_check(
    step0: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(xr, "H_MATCH", {**SYNTHETIC_H, "X4": SYNTHETIC_H["X4"] + 1})
    assert xr.check_step0(SERIES, ENTRIES, K, EXITS)["ok"] is False


def test_a_missing_k_or_day_fails_the_check(step0: None) -> None:
    assert xr.check_step0(SERIES, ENTRIES, None, EXITS)["ok"] is False
    assert xr.check_step0(SERIES._replace(missing=1), ENTRIES, K, EXITS)["ok"] is False


def test_the_real_constants_stop_the_table_before_any_return() -> None:
    result = xr.run_table(DATA, LOADED, GATE)
    assert result["complete"] is False
    assert result["check"]["ok"] is False
    assert set(result) == {
        "run_id",
        "owner_decision",
        "market",
        "window",
        "rule",
        "check",
        "complete",
    }


# The table -----------------------------------------------------------------------------------


def _expected_side(values: list[float], deepest: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    n = len(values)
    worst = ordered[: math.ceil(0.05 * n)]
    return {
        "trades": n,
        "mean": math.fsum(values) / n,
        "median": ordered[math.ceil(0.5 * n) - 1],
        "worst": ordered[0],
        "p05": ordered[math.ceil(0.05 * n) - 1],
        "cvar05": math.fsum(worst) / len(worst),
        "loss08": sum(v <= -0.08 for v in values),
        "loss15": sum(v <= -0.15 for v in values),
        "deepest_mean": math.fsum(deepest) / n,
        "deepest_p05": sorted(deepest)[math.ceil(0.05 * n) - 1],
    }


def _path(e: int, sell: int) -> tuple[float, float]:
    close = SERIES.close
    return close[sell] / close[e] - 1.0, min(close[e : sell + 1]) / close[e] - 1.0


def test_the_table_has_its_parts(table: dict[str, Any]) -> None:
    assert table["complete"] is True
    assert table["run_id"] == "TRADING-2565-109"
    assert table["market"] == "qqq"
    window = pt.market_window(LOADED, GATE, "qqq")
    assert table["window"] == {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": "2010-12-31",
    }
    assert table["rule"] == {
        "h_match": SYNTHETIC_H,
        "r60": 60,
        "tail": 0.05,
        "loss_lines": [0.08, 0.15],
        "after_days": 20,
        "year_days": 252,
        "gate_tail": 0.10,
        "gate_cost": 0.01,
        "sell": "next_close",
    }
    assert list(table["rules"]) == list(xc.RULES)
    assert table["check"]["ok"] is True


@pytest.mark.parametrize("rule", xc.RULES)
def test_each_rule_is_set_beside_its_matched_hold(table: dict[str, Any], rule: str) -> None:
    row = table["rules"][rule]
    hold = SYNTHETIC_H[rule]
    assert row["hold"] == hold
    made = [_path(e, EXITS[rule][e].sell) for e in ENTRIES]
    control = [_path(e, e + hold) for e in ENTRIES]
    mine = _expected_side([v for v, _ in made], [d for _, d in made])
    theirs = _expected_side([v for v, _ in control], [d for _, d in control])
    assert row["rule"] == pytest.approx(mine)
    assert row["control"] == pytest.approx(theirs)
    difference = mine["mean"] - theirs["mean"]
    assert row["compare"]["mean_difference"] == pytest.approx(difference)
    assert row["compare"]["per_year"] == pytest.approx(difference * 252 / hold)
    assert row["compare"]["cvar05_gain"] == pytest.approx(
        (mine["cvar05"] - theirs["cvar05"]) / abs(theirs["cvar05"])
    )


@pytest.mark.parametrize("rule", xc.RULES)
def test_the_exposure_is_the_rule_s_open_trades_over_h(table: dict[str, Any], rule: str) -> None:
    hold = SYNTHETIC_H[rule]
    start, end = ENTRIES[0] + 120, ENTRIES[-1] + 1
    ratios = [
        sum(e < t <= EXITS[rule][e].sell for e in ENTRIES) / hold for t in range(start, end + 1)
    ]
    ordered = sorted(ratios)
    n = len(ratios)
    assert table["rules"][rule]["exposure"] == pytest.approx(
        {
            "days": n,
            "mean": math.fsum(ratios) / n,
            "p05": ordered[math.ceil(0.05 * n) - 1],
            "p95": ordered[math.ceil(0.95 * n) - 1],
            "max": ordered[-1],
        }
    )
    control = [xr.Trade(e, e + hold, 0.0, 0.0) for e in ENTRIES]
    flat = xr.exposure(control, hold, start, end)
    assert (flat["p05"], flat["p95"], flat["max"], flat["mean"]) == (1.0, 1.0, 1.0, 1.0)


@pytest.mark.parametrize("rule", xc.RULES)
def test_false_exits_look_only_at_signal_sales(table: dict[str, Any], rule: str) -> None:
    close = SERIES.close
    sells = [x.sell for x in EXITS[rule].values() if x.reason == "signal"]
    kept = [s for s in sells if s + 20 <= len(close) - 1]
    up = sum(close[s + 20] > close[s] for s in kept)
    assert table["rules"][rule]["false_exit"] == {
        "n": len(kept),
        "up": up,
        "share": up / len(kept) if kept else None,
    }


def test_the_years_cover_every_entry(table: dict[str, Any]) -> None:
    for rule in xc.RULES:
        years = table["rules"][rule]["years"]
        assert [y["year"] for y in years] == list(range(2000, 2011))
        assert sum(y["trades"] for y in years) == len(ENTRIES)
    hold = SYNTHETIC_H["X3"]
    diffs = [
        _path(e, EXITS["X3"][e].sell)[0] - _path(e, e + hold)[0]
        for e in ENTRIES
        if SERIES.dates[e].year == 2008
    ]
    row = table["rules"]["X3"]["years"][8]
    assert row["year"] == 2008
    assert row["mean_difference"] == pytest.approx(math.fsum(diffs) / len(diffs))
    assert row["worst_difference"] == pytest.approx(min(diffs))


def test_the_sixty_day_row_and_the_base_rate(table: dict[str, Any]) -> None:
    made = [_path(e, e + 60) for e in ENTRIES]
    assert table["r60"] == pytest.approx(_expected_side([v for v, _ in made], [d for _, d in made]))
    close = SERIES.close
    up = sum(close[e + 20] > close[e] for e in ENTRIES)
    assert table["after_up_any_day"] == {"n": len(ENTRIES), "up": up, "share": up / len(ENTRIES)}


def test_entry_set_b_keeps_only_entries_in_range(table: dict[str, Any]) -> None:
    rows = table["entry_set_b"]
    in_range = [b["entry"] for b in STEP0["entry_set_b"] if b["in_range"]]
    assert [row["entry"] for row in rows] == in_range
    assert len(rows) == 2
    for row in rows:
        e = SERIES.dates.index(date.fromisoformat(row["entry"]))
        assert row["r60"] == pytest.approx(_path(e, e + 60)[0])
        for rule in xc.RULES:
            x = EXITS[rule][e]
            assert row["rules"][rule] == {
                "sell": SERIES.dates[x.sell].isoformat(),
                "value": pytest.approx(_path(e, x.sell)[0]),
                "control": pytest.approx(_path(e, e + SYNTHETIC_H[rule])[0]),
            }


def test_no_nan_reaches_the_table(table: dict[str, Any]) -> None:
    json.dumps(table, allow_nan=False)


# Text ----------------------------------------------------------------------------------------


def test_the_markdown_has_the_price_list_years_and_entry_set_b(table: dict[str, Any]) -> None:
    text = xr.render_markdown(table)
    lines = text.splitlines()
    assert lines[0] == "# QQQ 出场规则价目表（TRADING-2565-109）"
    assert lines[2] == xr.NOTE
    assert text.endswith("\n") and not text.endswith("\n\n")
    rows = [line for line in lines if line.startswith("| X")]
    assert [row.split(" | ")[0] for row in rows] == [f"| {r}" for r in xc.RULES]
    x0 = table["rules"]["X0"]
    assert rows[0].split(" | ")[2] == f"{100 * x0['compare']['per_year']:+.2f}"
    assert sum(line.startswith("| 20") for line in lines) == 11 + 2
    assert "per_trade_gate_size_like" in xr.NOTE
    assert "按笔" in xr.NOTE and "组合" in xr.NOTE and "几何" in xr.NOTE


def test_the_summary_line(table: dict[str, Any]) -> None:
    rules = table["rules"]
    yearly = "、".join(f"{r} {100 * rules[r]['compare']['per_year']:+.2f}" for r in xc.RULES)
    tails = "、".join(f"{r} {100 * rules[r]['compare']['cvar05_gain']:.1f}%" for r in xc.RULES)
    assert xr.summary_line(table, Path("D:/out")) == (
        f"exit_rules_returns: QQQ 1999–2010 {len(ENTRIES)} 笔；每年差（点）{yearly}；"
        f"CVaR5 改善 {tails}；结果目录 {Path('D:/out')}"
    )


# Command line --------------------------------------------------------------------------------


@pytest.fixture
def loaded_cli(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    roots: list[Path] = []

    def load_data(root: Path) -> Any:
        roots.append(root)
        return DATA, GATE, LOADED

    monkeypatch.setattr(xc, "load_data", load_data)
    return roots


def test_run_writes_both_files_and_prints_one_line(
    step0: None, loaded_cli: list[Path], table: dict[str, Any], tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(xr.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [tmp_path]
    text = out.joinpath("exit_rules_returns.json").read_text(encoding="utf-8")
    assert text == json.dumps(table, indent=2, ensure_ascii=False) + "\n"
    md = out.joinpath("exit_rules_returns.md").read_text(encoding="utf-8")
    assert md == xr.render_markdown(table)
    assert sorted(p.name for p in out.iterdir()) == [
        "exit_rules_returns.json",
        "exit_rules_returns.md",
    ]
    assert result.output == xr.summary_line(table, out) + "\n"


@pytest.mark.parametrize("name", ["exit_rules_returns.json", "exit_rules_returns.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    def load_data(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(xc, "load_data", load_data)
    tmp_path.joinpath(name).write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(xr.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_returns: 已有结果，拒绝覆盖。\n"
    assert [p.name for p in tmp_path.iterdir()] == [name]


def test_a_load_failure_writes_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def load_data(root: Path) -> Any:
        raise tr.TrialError("snapshot is not checked\nsecond line")

    monkeypatch.setattr(xc, "load_data", load_data)
    out = tmp_path / "out"
    result = CliRunner().invoke(xr.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_returns: 数据载入失败：snapshot is not checked\n"
    assert not out.exists()


def test_a_failed_check_writes_only_the_json(loaded_cli: list[Path], tmp_path: Path) -> None:
    result = CliRunner().invoke(xr.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_returns: 输入检查 FAIL，没有算收益\n"
    assert [p.name for p in tmp_path.iterdir()] == ["exit_rules_returns.json"]
    written = json.loads(tmp_path.joinpath("exit_rules_returns.json").read_text(encoding="utf-8"))
    assert written["complete"] is False
    assert "rules" not in written
