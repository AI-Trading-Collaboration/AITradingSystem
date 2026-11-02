"""Acceptance tests for TRADING-2565-111: the QQQ exit family's price list on the NDX 1986–1999.

Requirement: knowledge qqq-options/auto-loop/period7/exit-family/exit-family-ndx-price-list-
requirements-2026-11-02.md (owner decision exit_family_ndx_1986_1999). Synthetic data only; no
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
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import exit_rules_ndx as xn
from ai_trading_system.research_probes import exit_rules_returns as xr
from ai_trading_system.yaml_loader import safe_load_yaml_path

LOADED = load_settings()
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "exit_rules_ndx.py"
REGISTRY = PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
SOURCE = {"snapshot_id": "qlh-20261013-0b0df53cb1a2", "column": "ndx_price", "check_status": "WARN"}
WINDOW = en.registered_window(LOADED)


# Constants and imports ------------------------------------------------------------------------


def test_the_run_is_the_exit_family_on_qqqs_own_index() -> None:
    assert xn.RUN_ID == "TRADING-2565-111"
    assert xn.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-02:exit_family_ndx_1986_1999"
    assert xn.RULES == xc.RULES == ("X0", "X1", "X2", "X3", "X4", "X5", "X6")
    assert xn.RESULT_JSON == "exit_rules_ndx.json"
    assert xn.RESULT_MD == "exit_rules_ndx.md"


def test_the_stop_factor_and_qqq_holds_stay_frozen() -> None:
    assert xn.K == xr.STEP0_K == 4.421121529829901
    assert (
        xn.QQQ_H
        == xr.H_MATCH
        == {
            "X0": 44,
            "X1": 69,
            "X2": 42,
            "X3": 18,
            "X4": 89,
            "X5": 51,
            "X6": 42,
        }
    )


def test_the_data_check_is_the_print_of_trading_2565_107() -> None:
    assert (xn.NDX_CLOSES, xn.NDX_LAST) == (3317, "1999-03-09")
    assert xn.NDX_FLAT_DATES == (
        "1986-07-31",
        "1986-12-29",
        "1987-02-06",
        "1987-08-14",
        "1988-06-13",
        "1988-10-19",
        "1989-03-01",
        "1989-05-02",
        "1989-08-01",
        "1989-09-25",
        "1990-12-13",
        "1992-03-24",
        "1995-03-22",
        "1997-08-15",
        "1998-07-03",
        "1999-01-20",
    )
    assert (xn.NDX_MOVE_DATE, xn.NDX_MOVE) == ("1987-10-19", -0.1508)
    assert xn.MIN_TRADES == 2000


def test_the_qqq_readings_are_the_print_of_trading_2565_109() -> None:
    assert xn.QQQ_PER_YEAR == {
        "X0": 0.0630,
        "X1": 0.0359,
        "X2": 0.0201,
        "X3": 0.0191,
        "X4": -0.0509,
        "X5": 0.0553,
        "X6": 0.0144,
    }
    assert xn.QQQ_CVAR05_GAIN == {
        "X0": 0.5543,
        "X1": 0.3423,
        "X2": 0.3576,
        "X3": 0.4900,
        "X4": -0.1117,
        "X5": 0.4653,
        "X6": 0.1687,
    }


def test_the_module_reads_only_qqqs_own_index_through_the_registered_window() -> None:
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
        "datetime.",
        "pathlib.",
        "typing.",
        "typer",
        "ai_trading_system.research_data.qqq_long_history",
        "ai_trading_system.research_probes.entry_timing_counts",
        "ai_trading_system.research_probes.entry_timing_ndx",
        "ai_trading_system.research_probes.exit_rules_counts",
        "ai_trading_system.research_probes.exit_rules_returns",
    )
    for name in imported:
        assert name.startswith(allowed), name
    for needed in ("entry_timing_ndx", "exit_rules_counts", "exit_rules_returns"):
        assert f"ai_trading_system.research_probes.{needed}" in imported
    banned = (
        "ma200_forward",
        "window_reads",
        "ledger",
        ".unlink(",
        "urllib",
        "requests",
        "us_market",
        "ff_factors",
        "Mkt-RF",
        "etf_reference_return",
        "qqq_proxy_return",
        "ndx_tr",
        "policy_table",
        "xc.load_data",
        "xc.scale(",
        "def exit_for",
        "def prepare",
        "def trade",
        "def side",
        "def compare",
        "def exposure",
        "def after_up",
        "def by_year",
        "def prices",
        "safe_load_yaml",
        "load_daily",
    )
    for word in banned:
        assert word not in text, word
    assert text.count(".write_text(") == 2
    assert "en.load_data(" in text


def test_the_registry_note_names_this_reader_and_keeps_the_window() -> None:
    raw = safe_load_yaml_path(REGISTRY)
    entry = raw["windows"]["ndx_price_1986_1999"]
    assert (str(entry["start"]), str(entry["end"]), entry["role"]) == (
        "1986-01-02",
        "1999-03-09",
        "owner_print_only",
    )
    assert entry["question_types"] == []
    assert entry["read_policy"] == "once_per_owner_decision"
    assert entry["allowed_usage"] == ["owner_requested_print_only_read"]
    lines = REGISTRY.read_text(encoding="utf-8").splitlines()
    end = next(p for p, line in enumerate(lines) if line.strip() == "ndx_price_1986_1999:")
    start = end
    while lines[start - 1].lstrip().startswith("#"):
        start -= 1
    note = " ".join(line.strip().lstrip("#").strip() for line in lines[start:end])
    assert "research_probes/entry_timing_ndx.py" in note
    assert "research_probes/exit_rules_ndx.py" in note
    assert "TRADING-2565-111" in note
    assert "owner_decision:TRADING-2565:2026-11-02:exit_family_ndx_1986_1999" in note
    assert "exit family" in note


# Synthetic Nasdaq-100 levels ------------------------------------------------------------------

INDEX = pd.bdate_range("1986-01-02", "1999-03-09", name="date")
GROWTH = 1.0006
CRASHES = (("1987-10-12", "1987-10-23", 0.7), ("1990-07-16", "1990-10-12", 0.75))
DIPS = ("1992-03-02", "1994-04-04", "1996-07-15", "1998-08-31")
HOLIDAYS = ("1988-04-01", "1995-12-25")


def _pos(day: str) -> int:
    position = INDEX.get_loc(pd.Timestamp(day))
    assert isinstance(position, int)
    return position


def _path() -> np.ndarray:
    rng = np.random.default_rng(7)
    level = 100.0 * np.cumprod(GROWTH * (1.0 + rng.normal(0.0, 0.009, len(INDEX))))
    for first, last, depth in CRASHES:
        a, b = _pos(first), _pos(last)
        level[a : b + 1] *= np.linspace(1.0, depth, b - a + 1)
        level[b + 1 :] *= depth
    for day in DIPS:
        level[_pos(day) :] *= 0.93
    for day in HOLIDAYS:
        level[_pos(day)] = level[_pos(day) - 1]
    return level


def _levels(level: np.ndarray) -> list[tuple[date, float]]:
    return [(day.date(), float(value)) for day, value in zip(INDEX, level, strict=True)]


LEVELS = _levels(_path())
SERIES, FLAT = en.prices(LEVELS)
CLOSE = SERIES.close
PREPARED = xc.prepare(SERIES.dates, CLOSE)
ENTRIES = xc.entry_range(PREPARED, 0)
EXITS = {r: {e: xc.exit_for(r, PREPARED, e, xn.K) for e in ENTRIES} for r in xc.RULES}
MOVE = en.largest_move(SERIES)
assert MOVE is not None


@pytest.fixture
def same_data(monkeypatch: pytest.MonkeyPatch) -> None:
    assert MOVE is not None
    monkeypatch.setattr(xn, "NDX_CLOSES", len(SERIES.dates))
    monkeypatch.setattr(xn, "NDX_FLAT_DATES", tuple(day.isoformat() for day in FLAT))
    monkeypatch.setattr(xn, "NDX_MOVE_DATE", MOVE["date"])
    monkeypatch.setattr(xn, "NDX_MOVE", round(MOVE["return"], 4))


@pytest.fixture
def table(same_data: None) -> dict[str, Any]:
    return xn.run_table(LEVELS, SOURCE, WINDOW)


def test_the_synthetic_data_differ_from_trading_2565_107() -> None:
    assert MOVE is not None
    assert len(SERIES.dates) != xn.NDX_CLOSES
    assert tuple(day.isoformat() for day in FLAT) != xn.NDX_FLAT_DATES
    assert len(ENTRIES) >= xn.MIN_TRADES
    assert MOVE["date"] != xn.NDX_MOVE_DATE


# Input check ---------------------------------------------------------------------------------


def test_the_check_passes_on_the_same_data(same_data: None) -> None:
    check = xn.check_inputs(SERIES, FLAT, WINDOW, ENTRIES)
    assert check["ok"] is True
    assert check["window_id"] == "ndx_price_1986_1999"
    assert check["role"] == "owner_print_only"
    assert (check["start"], check["end"]) == ("1986-01-02", "1999-03-09")
    assert check["missing_days"] == 0
    assert check["closes"] == len(SERIES.dates)
    assert check["flat_days"] == len(FLAT) == len(HOLIDAYS)
    assert check["flat_dates"] == list(HOLIDAYS)
    assert (check["first_close_date"], check["last_close_date"]) == ("1986-01-02", "1999-03-09")
    assert check["largest_move"] == MOVE
    assert check["trades"] == len(ENTRIES)
    assert check["first_entry"] == SERIES.dates[ENTRIES[0]].isoformat()
    assert check["last_entry"] == SERIES.dates[ENTRIES[-1]].isoformat()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("NDX_CLOSES", 1),
        ("NDX_FLAT_DATES", ("1988-04-01",)),
        ("NDX_FLAT_DATES", ("1988-04-01", "1995-12-26")),
        ("NDX_LAST", "1999-03-08"),
        ("NDX_MOVE_DATE", "1987-10-20"),
        ("NDX_MOVE", -0.1509),
        ("MIN_TRADES", 10**6),
    ],
)
def test_each_check_against_the_earlier_print_can_fail(
    same_data: None, monkeypatch: pytest.MonkeyPatch, name: str, value: object
) -> None:
    monkeypatch.setattr(xn, name, value)
    assert xn.check_inputs(SERIES, FLAT, WINDOW, ENTRIES)["ok"] is False


def test_missing_levels_fail_the_check(same_data: None) -> None:
    assert xn.check_inputs(SERIES._replace(missing=1), FLAT, WINDOW, ENTRIES)["ok"] is False


def test_a_late_first_close_fails_the_check(same_data: None) -> None:
    late = SERIES._replace(dates=(date(1986, 1, 3), *SERIES.dates[1:]))
    assert xn.check_inputs(late, FLAT, WINDOW, ENTRIES)["ok"] is False


def test_an_early_last_close_fails_the_check(same_data: None) -> None:
    early = SERIES._replace(dates=(*SERIES.dates[:-1], date(1999, 3, 4)))
    assert xn.check_inputs(early, FLAT, WINDOW, ENTRIES)["ok"] is False


def test_the_real_constants_fail_on_synthetic_data_and_print_no_returns() -> None:
    result = xn.run_table(LEVELS, SOURCE, WINDOW)
    assert result["check"]["ok"] is False
    assert result["complete"] is False
    assert list(result) == ["run_id", "owner_decision", "source", "rule", "check", "complete"]
    assert "rules" not in result


# The table -----------------------------------------------------------------------------------


def test_the_result_carries_the_frozen_rule(table: dict[str, Any]) -> None:
    assert table["complete"] is True
    assert table["run_id"] == "TRADING-2565-111"
    assert table["source"] == SOURCE
    assert table["rule"] == {
        "k": 4.421121529829901,
        "qqq_h": xr.H_MATCH,
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


def test_the_holds_are_recomputed_here_from_sell_dates(table: dict[str, Any]) -> None:
    for rule in xc.RULES:
        row = table["rules"][rule]
        counts = xc.rule_counts(EXITS[rule])
        assert row["counts"] == counts
        assert row["hold"] == counts["h_match"]
        assert isinstance(row["hold"], int)
        assert row["qqq_hold"] == xr.H_MATCH[rule]
    assert any(table["rules"][r]["hold"] != xr.H_MATCH[r] for r in xc.RULES)


def _value(e: int, sell: int) -> tuple[float, float]:
    base = CLOSE[e]
    return CLOSE[sell] / base - 1.0, min(CLOSE[e : sell + 1]) / base - 1.0


def _expected_side(values: list[float], deepest: list[float]) -> dict[str, Any]:
    n = len(values)
    ordered, low = sorted(values), sorted(deepest)
    tail = max(math.ceil(0.05 * n), 1)
    return {
        "trades": n,
        "mean": math.fsum(values) / n,
        "median": ordered[max(math.ceil(0.5 * n) - 1, 0)],
        "worst": ordered[0],
        "p05": ordered[max(math.ceil(0.05 * n) - 1, 0)],
        "cvar05": math.fsum(ordered[:tail]) / tail,
        "loss08": sum(v <= -0.08 for v in values),
        "loss15": sum(v <= -0.15 for v in values),
        "deepest_mean": math.fsum(deepest) / n,
        "deepest_p05": low[max(math.ceil(0.05 * n) - 1, 0)],
    }


def test_every_cell_is_recomputed_from_the_closes(table: dict[str, Any]) -> None:
    for rule in xc.RULES:
        row = table["rules"][rule]
        hold = row["hold"]
        made = [_value(e, EXITS[rule][e].sell) for e in ENTRIES]
        held = [_value(e, e + hold) for e in ENTRIES]
        mine = _expected_side([v for v, _ in made], [d for _, d in made])
        theirs = _expected_side([v for v, _ in held], [d for _, d in held])
        assert row["rule"] == pytest.approx(mine, rel=1e-12, abs=1e-15)
        assert row["control"] == pytest.approx(theirs, rel=1e-12, abs=1e-15)
        difference = mine["mean"] - theirs["mean"]
        compare = row["compare"]
        assert compare["mean_difference"] == pytest.approx(difference, rel=1e-12, abs=1e-15)
        assert compare["per_year"] == pytest.approx(difference * 252 / hold, rel=1e-12, abs=1e-15)
        gain = (mine["cvar05"] - theirs["cvar05"]) / abs(theirs["cvar05"])
        assert compare["cvar05_gain"] == pytest.approx(gain, rel=1e-12)
        assert compare["per_trade_gate_size_like"] is (
            gain >= 0.10 and difference * 252 / hold >= -0.01
        )
        assert row["qqq"] == {
            "per_year": xn.QQQ_PER_YEAR[rule],
            "cvar05_gain": xn.QQQ_CVAR05_GAIN[rule],
        }


def test_the_stop_widths_are_printed_in_this_spans_volatility(table: dict[str, Any]) -> None:
    sigmas = [s for e in ENTRIES if (s := PREPARED.sigma[e]) is not None]
    mean = math.fsum(sigmas) / len(sigmas)
    distances = sorted(xn.K * s for s in sigmas)
    n = len(distances)
    assert table["scale"] == pytest.approx(
        {
            "sigma20_mean": mean,
            "x0_stop_in_sigma20": 0.08 / mean,
            "qqq_x0_stop_in_sigma20": xn.K,
            "x1_x2_distance_mean": math.fsum(distances) / n,
            "x1_x2_distance_p10": distances[max(math.ceil(0.10 * n) - 1, 0)],
            "x1_x2_distance_p90": distances[max(math.ceil(0.90 * n) - 1, 0)],
        },
        rel=1e-12,
    )
    assert list(table)[:6] == ["run_id", "owner_decision", "source", "rule", "check", "complete"]


def test_the_drift_from_exposure_is_printed_beside_the_yearly_difference(
    table: dict[str, Any],
) -> None:
    for rule in xc.RULES:
        row = table["rules"][rule]
        expected = (row["exposure"]["mean"] - 1.0) * row["control"]["mean"] * 252 / row["hold"]
        assert row["drift_from_exposure"] == pytest.approx(expected, rel=1e-12, abs=1e-15)
    assert any(abs(table["rules"][r]["drift_from_exposure"]) > 0 for r in xc.RULES)


def test_exposure_counts_open_trades_over_the_matched_hold(table: dict[str, Any]) -> None:
    start, end = ENTRIES[0] + xc.LONG_HOLD, ENTRIES[-1] + 1
    for rule in xc.RULES:
        hold = table["rules"][rule]["hold"]
        ratios = [
            sum(e < t <= EXITS[rule][e].sell for e in ENTRIES) / hold for t in range(start, end + 1)
        ]
        ordered = sorted(ratios)
        n = len(ratios)
        assert table["rules"][rule]["exposure"] == pytest.approx(
            {
                "days": n,
                "mean": math.fsum(ratios) / n,
                "p05": ordered[max(math.ceil(0.05 * n) - 1, 0)],
                "p95": ordered[max(math.ceil(0.95 * n) - 1, 0)],
                "max": ordered[-1],
            },
            rel=1e-12,
        )


def test_false_exits_count_strictly_higher_closes_after_signal_sales(
    table: dict[str, Any],
) -> None:
    for rule in xc.RULES:
        days = [EXITS[rule][e].sell for e in ENTRIES if EXITS[rule][e].reason == "signal"]
        kept = [d for d in days if d + 20 < len(CLOSE)]
        up = sum(CLOSE[d + 20] > CLOSE[d] for d in kept)
        assert table["rules"][rule]["false_exit"] == {
            "n": len(kept),
            "up": up,
            "share": up / len(kept) if kept else None,
        }
    every = [e for e in ENTRIES if e + 20 < len(CLOSE)]
    up = sum(CLOSE[e + 20] > CLOSE[e] for e in every)
    assert table["after_up_any_day"] == {"n": len(every), "up": up, "share": up / len(every)}


def test_years_split_the_difference_by_entry_year(table: dict[str, Any]) -> None:
    for rule in xc.RULES:
        hold = table["rules"][rule]["hold"]
        years: dict[int, list[float]] = {}
        for e in ENTRIES:
            diff = _value(e, EXITS[rule][e].sell)[0] - _value(e, e + hold)[0]
            years.setdefault(SERIES.dates[e].year, []).append(diff)
        expected = [
            {
                "year": y,
                "trades": len(v),
                "mean_difference": math.fsum(v) / len(v),
                "worst_difference": min(v),
            }
            for y, v in sorted(years.items())
        ]
        found = table["rules"][rule]["years"]
        assert len(found) == len(expected)
        for got, want in zip(found, expected, strict=True):
            assert got == pytest.approx(want, rel=1e-12, abs=1e-15)
    first = SERIES.dates[ENTRIES[0]].year
    assert table["rules"]["X0"]["years"][0]["year"] == first == 1986


def test_r60_is_the_sixty_session_hold(table: dict[str, Any]) -> None:
    made = [_value(e, e + 60) for e in ENTRIES]
    expected = _expected_side([v for v, _ in made], [d for _, d in made])
    assert table["r60"] == pytest.approx(expected, rel=1e-12, abs=1e-15)


def test_entry_set_b_keeps_only_counted_entries_in_range(table: dict[str, Any]) -> None:
    counted = [t.entry for t in ec.scan(CLOSE, 0) if t.counted and t.entry is not None]
    inside = [e for e in counted if e in ENTRIES]
    assert counted, "the synthetic path must give the 200-day rule something to count"
    assert [row["entry"] for row in table["entry_set_b"]] == [
        SERIES.dates[e].isoformat() for e in inside
    ]
    for row, e in zip(table["entry_set_b"], inside, strict=True):
        assert row["r60"] == pytest.approx(_value(e, e + 60)[0], rel=1e-12)
        for rule in xc.RULES:
            sell = EXITS[rule][e].sell
            hold = table["rules"][rule]["hold"]
            cell = row["rules"][rule]
            assert sorted(cell) == ["control", "sell", "value"]
            assert cell["sell"] == SERIES.dates[sell].isoformat()
            assert cell["value"] == pytest.approx(_value(e, sell)[0], rel=1e-12, abs=1e-15)
            assert cell["control"] == pytest.approx(_value(e, e + hold)[0], rel=1e-12, abs=1e-15)


def test_the_table_is_plain_json(table: dict[str, Any]) -> None:
    text = json.dumps(table, allow_nan=False)
    assert "NaN" not in text and "Infinity" not in text


# Markdown and the screen line ------------------------------------------------------------------


def test_the_markdown_prints_counts_price_list_years_and_entry_set_b(
    table: dict[str, Any],
) -> None:
    text = xn.render_markdown(table)
    lines = text.splitlines()
    assert lines[0] == "# QQQ 出场规则价目表：纳斯达克 100 价格指数 1986–1999（TRADING-2565-111）"
    assert xn.NOTE in text
    assert "不是过门" in xn.NOTE and "在场的时间点" in xn.NOTE and "k" in xn.NOTE
    for title in ("## 次数", "## 价目表（每条规则对拿 H 天）", "## 按入场年：每笔平均差（点）"):
        assert title in lines
    header = next(line for line in lines if line.startswith("| 规则 | H | 每年差"))
    assert header.count("|") == 23
    assert "平均仓位（倍）" in header and "仓位偏 1 的漂移（点）" in header
    width = table["scale"]
    assert f"X0 的 8% 约是 {width['x0_stop_in_sigma20']:.2f} 个 20 日波动" in text
    assert f"（QQQ 2000–2010 上 {xn.K:.2f} 个）" in text
    assert "QQQ 2000–2010 每年差（点）" in header
    for rule in xc.RULES:
        row = table["rules"][rule]
        counts = row["counts"]
        assert any(
            line.startswith(f"| {rule} | {counts['trades']} | {counts['signal']} |")
            and line.endswith(f"| {row['hold']} | {row['qqq_hold']} |")
            for line in lines
        )
        assert any(
            line.startswith(f"| {rule} | {row['hold']} | {xr.points(row['compare']['per_year'])} |")
            for line in lines
        )
        assert xr.points(xn.QQQ_PER_YEAR[rule]) in text
    for row in table["entry_set_b"]:
        assert any(line.startswith(f"| {row['entry']} |") for line in lines)
    years = {r["year"] for r in table["rules"]["X0"]["years"]}
    for year in years:
        assert any(line.startswith(f"| {year} |") for line in lines)
    assert text.endswith("\n") and not text.endswith("\n\n")


def test_the_summary_line_names_the_index_and_each_rule(table: dict[str, Any]) -> None:
    line = xn.summary_line(table, Path("out"))
    assert line.startswith(f"exit_rules_ndx: 纳斯达克 100 价格指数 1986–1999 {len(ENTRIES)} 笔；")
    for rule in xc.RULES:
        compare = table["rules"][rule]["compare"]
        assert f"{rule} {xr.points(compare['per_year'])}" in line
        assert f"{rule} {xr.share(compare['cvar05_gain'])}" in line
    assert line.endswith("结果目录 out")


# Command line --------------------------------------------------------------------------------


def _patch_load(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(en, "load_data", lambda root: (LEVELS, SOURCE, WINDOW))


def test_the_command_writes_both_files(
    same_data: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_load(monkeypatch)
    out = tmp_path / "out"
    result = CliRunner().invoke(xn.app, ["run", "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert result.output.startswith("exit_rules_ndx: 纳斯达克 100 价格指数 1986–1999 ")
    assert sorted(p.name for p in out.iterdir()) == ["exit_rules_ndx.json", "exit_rules_ndx.md"]
    data = json.loads(out.joinpath("exit_rules_ndx.json").read_text(encoding="utf-8"))
    assert data["complete"] is True
    text = out.joinpath("exit_rules_ndx.json").read_text(encoding="utf-8")
    assert text.endswith("}\n") and '  "run_id"' in text


@pytest.mark.parametrize("name", ["exit_rules_ndx.json", "exit_rules_ndx.md"])
def test_the_command_refuses_to_overwrite(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    calls: list[Path] = []

    def load(root: Path) -> Any:
        calls.append(root)
        return LEVELS, SOURCE, WINDOW

    monkeypatch.setattr(en, "load_data", load)
    tmp_path.joinpath(name).write_text("old", encoding="utf-8")
    result = CliRunner().invoke(xn.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert "已有结果，拒绝覆盖" in result.output
    assert calls == []
    assert tmp_path.joinpath(name).read_text(encoding="utf-8") == "old"


def test_a_load_failure_writes_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def fail(root: Path) -> Any:
        raise ValueError("窗口登记表里没有 ndx_price_1986_1999。\n第二行")

    monkeypatch.setattr(en, "load_data", fail)
    out = tmp_path / "out"
    result = CliRunner().invoke(xn.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_ndx: 数据载入失败：窗口登记表里没有 ndx_price_1986_1999。\n"
    assert not out.exists()


def test_a_failed_check_writes_only_the_json(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_load(monkeypatch)
    out = tmp_path / "out"
    result = CliRunner().invoke(xn.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    assert result.output == "exit_rules_ndx: 输入检查 FAIL，没有算收益\n"
    assert [p.name for p in out.iterdir()] == ["exit_rules_ndx.json"]
    data = json.loads(out.joinpath("exit_rules_ndx.json").read_text(encoding="utf-8"))
    assert data["complete"] is False and data["check"]["ok"] is False
