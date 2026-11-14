"""Acceptance tests for TRADING-2565-134: the three-span same-drawdown table.

Requirement: knowledge research-review/same-drawdown-table-requirements-2026-11-14.md v1.3
(v1.2 plus the package review: the bear rows' same-drawdown pair and the reproduction right
after the read; owner decision same_drawdown_table). Synthetic data only; no snapshot, no network.
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
from ai_trading_system.research_bench import segments as sg
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.metrics import summarize
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_probes import entry_exit_combo as cb
from ai_trading_system.research_probes import entry_structure_shift as es
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import leverage_table as lt
from ai_trading_system.research_probes import same_drawdown_table as sd
from ai_trading_system.research_probes import structure_shift_ladder as sl
from ai_trading_system.research_probes import structure_shift_periods as sp
from ai_trading_system.yaml_loader import safe_load_yaml_path

LOADED = load_settings()
ROOT_DIR = lg.research_root(LOADED)
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "same_drawdown_table.py"
REGISTRY = PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
REQUIREMENTS = PROJECT_ROOT / "docs" / "requirements" / "TRADING-2565_Research_Bench.md"
SYSTEM_FLOW = PROJECT_ROOT / "docs" / "system_flow.md"
COARSE_F = tuple(round(i / 10, 1) for i in range(11))
COARSE_L = tuple(round(i / 10, 1) for i in range(1, 31))
# TRADING-2565-126's printed 1999-12-23..2010-12-31 rows (structure_shift_ladder.json).
PRINTED_126 = {
    "x2_s5_under": {"total": 0.985413, "cagr": 0.064197, "max_drawdown": -0.394572},
    "x2_s5_under_ladder": {"total": 1.336565, "cagr": 0.080036, "max_drawdown": -0.460844},
    "hold": {"total": -0.370499, "cagr": -0.04112, "max_drawdown": -0.829809},
}
# TRADING-2565-122's printed x2_s5_under and hold rows on the two windows
# (structure_shift_periods.json) and TRADING-2565-126's ladder row on QQQ after 2010
# (structure_shift_ladder.json), at six decimals.
PRINTED_READ: dict[str, dict[str, Any]] = {
    "ndx": {
        "first_day": "1986-10-17",
        "last_day": "1999-03-09",
        "sessions": 3116,
        "rows": {
            "x2_s5_under": {"total": 6.206591, "cagr": 0.172787, "max_drawdown": -0.321774},
            "hold": {"total": 13.360477, "cagr": 0.239892, "max_drawdown": -0.399327},
        },
    },
    "qqq": {
        "first_day": "2011-01-03",
        "last_day": "2026-10-12",
        "sessions": 3966,
        "rows": {
            "x2_s5_under": {"total": 4.842585, "cagr": 0.118415, "max_drawdown": -0.385742},
            "x2_s5_under_ladder": {"total": 5.359274, "cagr": 0.12444, "max_drawdown": -0.378259},
            "hold": {"total": 14.43185, "cagr": 0.18945, "max_drawdown": -0.351178},
        },
    },
}


def _span(
    close: list[float],
    *,
    cash: float | list[float] = 0.0001,
    f0001: list[float] | None = None,
    first_in: int = 0,
    start: int = 0,
    first_day: str = "2020-01-27",
    name: str = "qqq",
) -> sd.Span:
    dates = tuple(d.date() for d in pd.bdate_range(first_day, periods=len(close)))
    rates = [math.nan] + ([cash] * (len(close) - 1) if isinstance(cash, float) else cash[1:])
    targets = f0001 if f0001 is not None else [0.5] * len(close)
    return sd.Span(name, dates, tuple(close), tuple(rates), tuple(targets), first_in, start)


def _walk(n: int, seed: int) -> list[float]:
    rng = np.random.default_rng(seed)
    r = rng.normal(0.0006, 0.013, n)
    r[600:700] -= 0.006
    r[1000:1040] -= 0.01
    return [float(x) for x in np.cumprod(1 + r) * 100]


def _synthetic(seed: int, n: int = 1400, start: int = 450) -> sd.Span:
    close = _walk(n, seed)
    rng = np.random.default_rng(seed + 100)
    cash = [math.nan] + [0.00012 * (1 + (i % 7) / 10) for i in range(1, n)]
    targets = [math.nan] * 260 + [float(x) for x in rng.choice([0.0, 0.5, 1.0], n - 260)]
    return _span(close, cash=cash, f0001=targets, first_in=250, start=start, first_day="2001-01-02")


def _m(dd: float, cagr: float, vol: float = 0.2) -> dict[str, Any]:
    return {"max_drawdown": dd, "cagr": cagr, "volatility": vol}


# Constants, imports and the registry -----------------------------------------------------------


def test_the_run_its_spans_lines_and_grids() -> None:
    assert sd.RUN_ID == "TRADING-2565-134"
    assert sd.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-10:same_drawdown_table"
    assert sd.SPANS == ("ndx", "dev", "qqq")
    assert sd.LINES == ("hold", "f0001", "x2_s5_under", "x2_s5_under_ladder")
    assert sd.RULED == ("f0001", "x2_s5_under", "x2_s5_under_ladder")
    assert sd.SHIFT_LINES == ("x2_s5_under", "x2_s5_under_ladder")
    assert sd.COMMON_START == {
        "ndx": date(1987, 1, 2),
        "dev": date(1999, 12, 23),
        "qqq": date(2011, 1, 3),
    }
    assert len(sd.F_GRID) == 101 and sd.F_GRID[0] == 0.0 and sd.F_GRID[-1] == 1.0
    assert all(math.isclose(b - a, 0.01) for a, b in zip(sd.F_GRID, sd.F_GRID[1:], strict=False))
    assert len(sd.L_GRID) == 300 and sd.L_GRID[0] == 0.01 and sd.L_GRID[-1] == 3.0
    assert sd.L_GRID[99] == 1.0
    assert all(math.isclose(b - a, 0.01) for a, b in zip(sd.L_GRID, sd.L_GRID[1:], strict=False))
    assert sd.LINE_COST == cb.COST == 0.0005
    assert (lt.MAIN.spread, lt.MAIN.bps, lt.ROBUST.spread, lt.ROBUST.bps) == (0.01, 4.0, 0.02, 25.0)
    assert sd.PICK_MARGIN == 0.001
    assert sd.SIMPLER == "x2_s5_under"
    assert sd.TRADING_DAYS == 252
    assert (sd.LEVERAGE_START, sd.LEVERAGE_END) == (date(1999, 3, 10), date(2010, 12, 31))
    assert (sd.CHECK_JSON, sd.RESULT_JSON, sd.RESULT_MD) == (
        "same_drawdown_table_check.json",
        "same_drawdown_table.json",
        "same_drawdown_table.md",
    )


def test_the_reproduction_pins_are_the_126_printed_rows() -> None:
    assert sd.DEV_PRINTED == PRINTED_126


def test_the_read_pins_are_the_122_and_126_printed_rows() -> None:
    assert sd.READ_PRINTED == PRINTED_READ
    assert (sd.DEV_FIRST_DAY, sd.DEV_SESSIONS) == ("1999-12-23", 2771)


def test_the_windows_assets_and_columns() -> None:
    assert sd.NDX_ASSETS == ("NDX_PRICE_INDEX", "CASH_DTB3")
    assert sd.QQQ_ASSETS == ("QQQ", "CASH_DTB3", "NDX_RETURN")
    assert sd.NDX_COLUMNS == ("ndx_price", "cash_return")
    assert sd.QQQ_COLUMNS == ("etf_reference_return", "cash_return", "ndx_return")


def _source() -> str:
    return MODULE.read_text(encoding="utf-8")


def test_the_module_reads_only_qqq_its_index_and_cash() -> None:
    text = _source()
    for word in ("us_market", "ff_factors", "ken_french", "US_MARKET", "load_data", "MARKETS"):
        assert word not in text, word
    tree = ast.parse(text)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert "policy_table" not in imported
    assert "leverage_table" in imported
    assert "structure_shift_periods" in imported and "structure_shift_ladder" in imported


def test_the_module_reuses_the_existing_rules_unchanged() -> None:
    text = _source()
    for call in ("sp.walk(", "sl.walk_ladder(", "sp.exit_under(", "es.shifts(", "sp.v0_entries("):
        assert call in text, call
    for call in ("lt.simulate_levered(", "lt.f0001_targets(", "lt.largest_allowed("):
        assert call in text, call
    for literal in ("0.08", "0.9", "200,"):
        assert f"= {literal}" not in text


def _windows() -> dict[str, Any]:
    raw = safe_load_yaml_path(REGISTRY)
    assert isinstance(raw, dict)
    windows = raw["windows"]
    assert isinstance(windows, dict)
    return windows


def test_the_two_windows_list_the_added_columns_and_keep_the_rest() -> None:
    windows = _windows()
    ndx, qqq = windows["ndx_price_1986_1999"], windows["qqq_after_2010_owner_print"]
    assert ndx["assets"] == ["NDX_PRICE_INDEX", "CASH_DTB3"]
    assert qqq["assets"] == ["QQQ", "CASH_DTB3", "NDX_RETURN"]
    assert (str(ndx["start"]), str(ndx["end"]), ndx["role"], ndx["read_policy"]) == (
        "1986-01-02",
        "1999-03-09",
        "owner_print_only",
        "once_per_owner_decision",
    )
    assert (str(qqq["start"]), qqq["end"], qqq["role"], qqq["read_policy"]) == (
        "2011-01-03",
        "latest",
        "owner_print_only",
        "once_per_owner_decision",
    )
    assert (ndx["data_column"], qqq["data_column"]) == ("ndx_price", "etf_reference_return")
    assert ndx["allowed_usage"] == qqq["allowed_usage"] == ["owner_requested_print_only_read"]
    assert ndx["blocked_usage"] == qqq["blocked_usage"]
    assert ndx["question_types"] == qqq["question_types"] == []
    assert windows["qqq_long_history_development"]["assets"] == [
        "QQQ_PROXY_NDX_TR",
        "CASH_DTB3",
        "SYNTHETIC_2X",
        "SYNTHETIC_3X",
    ]


def _note(start: str, end: str) -> str:
    text = REGISTRY.read_text(encoding="utf-8")
    block = text[text.index(start) :]
    return " ".join(block[: block.index(end)].replace("#", " ").split())


def test_the_registry_notes_name_this_reader_and_its_columns() -> None:
    ndx = _note("# Baker's print-only read of the 200-day pullback rule", "  ndx_price_1986_1999:")
    for words in (
        "seventh read",
        "TRADING-2565-134",
        sd.OWNER_DECISION,
        "research_probes/same_drawdown_table.py",
        "cash_return",
        "CASH_DTB3",
        "first read of the span",
    ):
        assert words in ndx, words
    assert ndx.index("sixth read") < ndx.index("seventh read")
    assert "TRADING-2565-126" not in ndx
    qqq = _note(
        "# Baker's one-time print-only read of QQQ after 2010", "  qqq_after_2010_owner_print:"
    )
    for words in (
        "fifth read",
        "TRADING-2565-134",
        sd.OWNER_DECISION,
        "research_probes/same_drawdown_table.py",
        "cash_return",
        "ndx_return",
        "CASH_DTB3",
        "NDX_RETURN",
        "forward ledger",
    ):
        assert words in qqq, words
    assert qqq.index("fourth read") < qqq.index("fifth read")


def _loaded_with(tmp_path: Path, changes: dict[str, dict[str, Any]]) -> LoadedSettings:
    raw = safe_load_yaml_path(REGISTRY)
    assert isinstance(raw, dict)
    for window_id, entry in changes.items():
        raw["windows"][window_id] = {**raw["windows"][window_id], **entry}
    tmp_path.joinpath("registry.yaml").write_text(
        json.dumps(raw, default=str, ensure_ascii=False), encoding="utf-8"
    )
    settings = LOADED.settings.model_copy(update={"window_registry": "registry.yaml"})
    return LoadedSettings(settings=settings, root=tmp_path, sha256=LOADED.sha256)


def test_the_registered_windows_carry_the_read_columns() -> None:
    windows = sd.registered_windows(LOADED)
    assert windows["ndx"]["window_id"] == "ndx_price_1986_1999"
    assert windows["qqq"]["window_id"] == "qqq_after_2010_owner_print"
    assert windows["ndx"]["columns"] == ["ndx_price", "cash_return"]
    assert windows["qqq"]["columns"] == ["etf_reference_return", "cash_return", "ndx_return"]
    assert windows["ndx"]["assets"] == ["NDX_PRICE_INDEX", "CASH_DTB3"]
    assert windows["qqq"]["assets"] == ["QQQ", "CASH_DTB3", "NDX_RETURN"]
    assert windows["ndx"]["read_policy"] == windows["qqq"]["read_policy"] == sd.READ_POLICY


@pytest.mark.parametrize(
    ("window_id", "entry"),
    [
        ("ndx_price_1986_1999", {"assets": ["NDX_PRICE_INDEX"]}),
        ("ndx_price_1986_1999", {"assets": ["NDX_PRICE_INDEX", "CASH_DTB3", "VIX"]}),
        ("ndx_price_1986_1999", {"read_policy": "unlimited"}),
        ("ndx_price_1986_1999", {"data_column": "ndx_tr"}),
        ("qqq_after_2010_owner_print", {"assets": ["QQQ"]}),
        ("qqq_after_2010_owner_print", {"assets": ["QQQ", "CASH_DTB3"]}),
        ("qqq_after_2010_owner_print", {"data_column": "qqq_proxy_return"}),
    ],
)
def test_a_window_registered_otherwise_refuses_the_read(
    tmp_path: Path, window_id: str, entry: dict[str, Any]
) -> None:
    with pytest.raises(ValueError, match=f"窗口 {window_id} 的登记和本表不一致"):
        sd.registered_windows(_loaded_with(tmp_path, {window_id: entry}))


def test_a_moved_window_refuses_through_the_original_checks(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="ndx_price_1986_1999"):
        sd.registered_windows(
            _loaded_with(tmp_path, {"ndx_price_1986_1999": {"end": "1999-03-10"}})
        )
    with pytest.raises(ValueError, match="qqq_after_2010_owner_print"):
        sd.registered_windows(
            _loaded_with(tmp_path, {"qqq_after_2010_owner_print": {"start": "2011-01-04"}})
        )


# Only the listed columns -----------------------------------------------------------------------


def _daily(days: list[str], price: list[float], cash: list[float]) -> pd.DataFrame:
    index = pd.DatetimeIndex(pd.to_datetime(days), name="date")
    return pd.DataFrame(
        {
            "ndx_price": price,
            "cash_return": cash,
            "vix": [math.nan] * len(days),
            "etf_reference_return": [0.5] * len(days),
            "lev2_return": [math.nan] * len(days),
        },
        index=index,
    )


def test_the_index_frame_keeps_two_columns_inside_the_window() -> None:
    daily = _daily(
        ["1985-12-31", "1986-01-02", "1999-03-09", "1999-03-10"],
        [1.0, 2.0, 3.0, 4.0],
        [0.1, 0.2, 0.3, 0.4],
    )
    frame = sd.ndx_frame(daily)
    assert list(frame.columns) == ["ndx_price", "cash_return"]
    assert [d.date() for d in frame.index] == [date(1986, 1, 2), date(1999, 3, 9)]
    assert sd.ndx_levels(frame) == [(date(1986, 1, 2), 2.0), (date(1999, 3, 9), 3.0)]


def test_the_qqq_data_keeps_qqq_cash_and_the_signal_input() -> None:
    index = pd.bdate_range("2011-01-03", periods=3)
    data = BenchData(
        returns=pd.DataFrame({"qqq": [0.1] * 3, "cash": [0.0] * 3, "lev2": [9.0] * 3}, index=index),
        inputs=pd.DataFrame(
            {"ndx_return": [0.1] * 3, "vix": [20.0] * 3, "qqq_return": [0.1] * 3}, index=index
        ),
        source={"snapshot_id": "x"},
    )
    kept = sd.qqq_data(data)
    assert list(kept.returns.columns) == ["qqq", "cash"]
    assert list(kept.inputs.columns) == ["ndx_return"]
    assert kept.source == {"snapshot_id": "x"}


def test_index_cash_compounds_over_the_rows_a_flat_close_drops() -> None:
    frame = _daily(
        ["1986-01-02", "1986-01-03", "1986-01-06", "1986-01-07"],
        [100.0, 101.0, 101.0, 102.0],
        [0.5, 0.01, 0.02, 0.03],
    ).loc[:, ["ndx_price", "cash_return"]]
    series, flat = en.prices(sd.ndx_levels(frame))
    assert flat == (date(1986, 1, 6),)
    cash = sd.ndx_cash(frame, series.dates)
    assert math.isnan(cash[0])
    assert cash[1] == pytest.approx(0.01)
    assert cash[2] == pytest.approx(1.02 * 1.03 - 1.0)


def test_f0001_is_given_only_the_signal_input(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[list[str]] = []

    def targets(data: BenchData, root_dir: Path) -> pd.Series:
        seen.append(list(data.inputs.columns))
        return pd.Series(0.5, index=data.inputs.index)

    monkeypatch.setattr(lt, "f0001_targets", targets)
    frame = _daily(["1986-01-02", "1986-01-03", "1986-01-06"], [100.0, 101.0, 101.0], [0, 0, 0])
    result = sd.ndx_targets(frame.loc[:, ["ndx_price", "cash_return"]], PROJECT_ROOT)
    assert seen == [["ndx_return"]]
    assert list(result) == [0.5, 0.5, 0.5]


def test_index_f0001_reads_the_price_return_on_the_snapshot_calendar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[pd.Series] = []

    def targets(data: BenchData, root_dir: Path) -> pd.Series:
        captured.append(data.inputs["ndx_return"])
        return pd.Series(1.0, index=data.inputs.index)

    monkeypatch.setattr(lt, "f0001_targets", targets)
    frame = _daily(
        ["1986-01-02", "1986-01-03", "1986-01-06", "1986-01-07"],
        [100.0, 110.0, 110.0, 99.0],
        [0, 0, 0, 0],
    ).loc[:, ["ndx_price", "cash_return"]]
    sd.ndx_targets(frame, PROJECT_ROOT)
    found = captured[0].to_numpy(dtype=float)
    assert math.isnan(found[0])
    assert list(found[1:]) == pytest.approx([0.1, 0.0, -0.1])


def test_f0001_on_the_index_uses_the_registered_strategy() -> None:
    days = pd.bdate_range("1986-01-02", periods=300)
    price = np.cumprod(np.full(300, 1.001)) * 100
    frame = pd.DataFrame({"ndx_price": price, "cash_return": 0.0}, index=days)
    found = sd.ndx_targets(frame, ROOT_DIR)
    assert found.iloc[:252].isna().all()
    assert found.iloc[252:].eq(1.0).all()


# Common start ----------------------------------------------------------------------------------


def test_a_span_starts_at_its_common_start() -> None:
    days = pd.bdate_range("1999-12-20", periods=8)
    series = ec.Closes(tuple(d.date() for d in days), tuple(100.0 + i for i in range(8)), 0)
    span = sd.make_span("dev", series, [math.nan] + [0.0] * 7, [0.5] * 8, 0)
    assert span.dates[span.start] == date(1999, 12, 23)
    with pytest.raises(ValueError, match="不是收盘日"):
        sd.make_span("qqq", series, [math.nan] + [0.0] * 7, [0.5] * 8, 0)


def test_measures_begin_at_the_common_start() -> None:
    # 2020-01-31 (position 1) is the month-end that opens the position before the start.
    span = _span([100.0, 50.0, 60.0, 66.0, 60.0], start=2, first_day="2020-01-30")
    hold = sd.levered_run(span, [1.0] * 5, 1.0, lt.MAIN)
    assert len(hold.returns) == 2
    m = sd.measure(span, hold)
    assert m["total"] == pytest.approx(60.0 / 60.0 - 1.0)
    assert m["max_drawdown"] == pytest.approx(60.0 / 66.0 - 1.0)
    assert m["drawdown_peak"] == span.dates[3].isoformat()


def test_state_before_the_start_is_computed_but_not_measured() -> None:
    span = _span([100.0, 101.0, 99.0, 102.0, 103.0, 104.0], start=3)
    flows = sd.Flows((1.0, 1.0, 0.0, 0.0, 0.0, 0.0), (1.0, 0.0, 1.0, 0.0, 0.0, 0.0))
    run = sd.scaled_run(span, flows, 1.0, lt.MAIN)
    assert run.exposure[0] == 0.0
    assert run.returns == pytest.approx((0.0001, 0.0001))
    flows_in = sd.Flows((1.0,) * 6, (1.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    run_in = sd.scaled_run(span, flows_in, 1.0, lt.MAIN)
    assert run_in.exposure[0] == 1.0
    assert run_in.returns[0] == pytest.approx(103.0 / 102.0 - 1.0)


# Money with cash ------------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_the_routes_are_the_frozen_rules(seed: int) -> None:
    span = _synthetic(seed)
    close, dates = span.close, span.dates
    line = ec.moving_average(close)
    s0 = max(span.first_in, ec.SMA_DAYS)
    prepared = xc.prepare(dates, close)
    entries = sp.v0_entries(close, span.first_in)
    moves = es.shifts(close, line, 5, s0)

    def fire(rule: str, data: xc.Prepared, b: int) -> tuple[int, str] | None:
        return sp.exit_under(rule, data, line, b)

    found = sd.routes(span)
    assert found["x2_s5_under"] == (sp.walk("x2", prepared, entries, moves, s0, fire), s0)
    ladder = sl.walk_ladder(prepared, entries, moves, s0, fire)
    assert found["x2_s5_under_ladder"] == (ladder.route, s0)


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5, 6, 7])
def test_money_at_zero_cash_is_the_printed_equity(seed: int) -> None:
    span = _synthetic(seed)
    zero = [0.0] * len(span.close)
    found = sd.routes(span)
    route, s0 = found["x2_s5_under"]
    values, _ = sd.replay(span.dates, span.close, zero, route, s0)
    assert values == pytest.approx(cb.equity(span.close, route, s0), rel=1e-12)
    line = ec.moving_average(span.close)
    ladder = sl.walk_ladder(
        xc.prepare(span.dates, span.close),
        sp.v0_entries(span.close, span.first_in),
        es.shifts(span.close, line, 5, s0),
        s0,
        lambda rule, data, b: sp.exit_under(rule, data, line, b),
    )
    values, _ = sd.replay(span.dates, span.close, zero, ladder.route, s0)
    assert values == pytest.approx(ladder.values, rel=1e-12)
    hold, h0 = sd.hold_route(span)
    values, _ = sd.replay(span.dates, span.close, zero, hold, h0)
    assert values == pytest.approx(cb.equity(span.close, hold, h0), rel=1e-12)


def _route(
    sells: tuple[int, ...], trips: tuple[dict[str, Any], ...], buys: tuple[int, ...]
) -> cb.Route:
    return cb.Route((), buys, sells, trips)


def test_flat_cash_earns_its_return_and_trades_pay_the_line_cost() -> None:
    span = _span([100.0, 102.0, 101.0, 99.0, 103.0], cash=0.001)
    d = span.dates
    trip = {"sell": d[2].isoformat(), "buy": d[4].isoformat(), "how": "entry"}
    values, flows = sd.replay(d, span.close, span.cash, _route((2,), (trip,), (0, 4)), 0)
    v0 = 1.0 - 0.0005
    v1 = v0 * 1.02
    v2 = v1 * (101.0 / 102.0) * (1.0 - 0.0005)
    v3 = v2 * 1.001
    v4 = v3 * 1.001 * (1.0 - 0.0005)
    assert values == pytest.approx([v0, v1, v2, v3, v4], rel=1e-12)
    assert flows.weight == pytest.approx((1.0, 1.0, 0.0, 0.0, 1.0))
    assert flows.traded == pytest.approx((1.0, 0.0, 1.0, 0.0, 1.0))


def test_tranches_spend_their_share_of_the_cash_at_sale() -> None:
    close = [100.0, 100.0, 89.0, 79.0, 85.0, 101.0]
    span = _span(close, cash=0.001)
    d = span.dates
    trip = {
        "sell": d[1].isoformat(),
        "buy": d[5].isoformat(),
        "how": "recovery",
        "fills": [
            {"level": 0.9, "date": d[2].isoformat(), "share": 0.1},
            {"level": 0.8, "date": d[3].isoformat(), "share": 0.2},
        ],
    }
    values, flows = sd.replay(d, close, span.cash, _route((1,), (trip,), (0, 2, 3, 5)), 0)
    cost = 0.0005
    shares = (1 - cost) / 100.0
    money = shares * 100.0 * (1 - cost)
    start = money
    shares = 0.0
    money *= 1.001
    spend2 = 0.1 * start
    shares += spend2 * (1 - cost) / 89.0
    money -= spend2
    v2 = shares * 89.0 + money
    money *= 1.001
    before3 = shares * 79.0 + money
    spend3 = 0.2 * start
    shares += spend3 * (1 - cost) / 79.0
    money -= spend3
    v3 = shares * 79.0 + money
    money *= 1.001
    v4 = shares * 85.0 + money
    w4 = shares * 85.0 / v4
    money *= 1.001
    before5 = shares * 101.0 + money
    shares += money * (1 - cost) / 101.0
    spend5, money = money, 0.0
    v5 = shares * 101.0
    assert values[2:] == pytest.approx([v2, v3, v4, v5], rel=1e-12)
    assert flows.traded[3] == pytest.approx(spend3 / before3)
    assert flows.traded[5] == pytest.approx(spend5 / before5)
    assert flows.weight[4] == pytest.approx(w4)
    assert flows.weight[5] == pytest.approx(1.0)


def test_the_fourth_tranche_spends_all_the_cash() -> None:
    close = [100.0, 100.0, 59.0, 70.0]
    span = _span(close, cash=0.001)
    d = span.dates
    fills = [
        {"level": level, "date": d[2].isoformat(), "share": share} for level, share in sl.LEVELS
    ]
    trip = {"sell": d[1].isoformat(), "buy": d[2].isoformat(), "how": "ladder", "fills": fills}
    _, flows = sd.replay(d, close, span.cash, _route((1,), (trip,), (0, 2)), 0)
    assert flows.weight[2] == pytest.approx(1.0)
    assert flows.traded[2] == pytest.approx(1.0)


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_one_times_the_line_is_the_line(seed: int) -> None:
    span = _synthetic(seed)
    for route, s0 in sd.routes(span).values():
        values, flows = sd.replay(span.dates, span.close, span.cash, route, s0)
        run = sd.scaled_run(span, flows, 1.0, lt.MAIN)
        equity = np.cumprod(1.0 + np.asarray(run.returns))
        expected = np.asarray(values[span.start - s0 + 1 :]) / values[span.start - s0]
        assert equity == pytest.approx(expected, rel=1e-10)


# Leverage on a structure-shift line ------------------------------------------------------------


def test_leverage_drifts_and_resets_at_month_end_with_spread() -> None:
    close = [100.0, 101.0, 99.0, 102.0, 100.0, 103.0, 104.0]
    span = _span(close, cash=0.0002)
    assert span.dates[4] == date(2020, 1, 31)
    flows = sd.Flows((0.75,) * 7, (0.0,) * 7)
    for costs in (lt.MAIN, lt.ROBUST):
        run = sd.scaled_run(span, flows, 2.0, costs)
        w = 1.5
        expected, exposure = [], []
        for q in range(1, 7):
            market = close[q] / close[q - 1] - 1.0
            exposure.append(w)
            spread = max(w - 1.0, 0.0) * costs.spread / 252
            gross = w * market + (1.0 - w) * 0.0002 - spread
            w = w * (1.0 + market) / (1.0 + gross)
            cost = 0.0
            if q == 4:
                assert w != pytest.approx(1.5, rel=1e-4)
                cost = abs(1.5 - w) * costs.bps / 10_000
                w = 1.5
            expected.append((1.0 + gross) * (1.0 - cost) - 1.0)
        assert run.exposure == pytest.approx(exposure, rel=1e-12)
        assert run.returns == pytest.approx(expected, rel=1e-12)
        assert run.exposure[4] == 1.5
        assert run.trades == 0


def test_a_line_trade_resets_and_pays_the_line_cost_times_leverage() -> None:
    close = [100.0, 102.0, 104.0, 103.0]
    span = _span(close, cash=0.0)
    flows = sd.Flows((1.0, 1.0, 0.4, 0.4), (1.0, 0.0, 0.6, 0.0))
    run = sd.scaled_run(span, flows, 1.5, lt.ROBUST)
    w = 1.5
    m1 = 0.02
    g1 = w * m1 - 0.5 * 0.02 / 252
    w = w * (1 + m1) / (1 + g1)
    m2 = 104.0 / 102.0 - 1.0
    g2 = w * m2 - (w - 1.0) * 0.02 / 252
    r2 = (1 + g2) * (1 - 0.0005 * 1.5 * 0.6) - 1
    assert run.returns[1] == pytest.approx(r2, rel=1e-12)
    assert run.exposure[2] == pytest.approx(0.6)
    assert run.trades == 1


def test_below_one_the_rest_is_cash_and_no_spread() -> None:
    close = [100.0, 110.0, 99.0]
    span = _span(close, cash=0.001)
    flows = sd.Flows((1.0, 1.0, 1.0), (1.0, 0.0, 0.0))
    run = sd.scaled_run(span, flows, 0.4, lt.ROBUST)
    assert run.returns[0] == pytest.approx(0.4 * 0.1 + 0.6 * 0.001)
    w = 0.4 * 1.1 / (1 + 0.4 * 0.1 + 0.6 * 0.001)
    assert run.exposure[1] == pytest.approx(w)


def test_a_bust_ends_the_run() -> None:
    close = [100.0, 60.0, 70.0, 80.0]
    span = _span(close, cash=0.0)
    flows = sd.Flows((1.0,) * 4, (1.0, 0.0, 0.0, 0.0))
    run = sd.scaled_run(span, flows, 3.0, lt.MAIN)
    assert run.returns[0] == -1.0
    assert run.returns[1:] == (0.0, 0.0)
    assert run.bust == span.dates[1].isoformat()


# Hold, F0001 and fixed ------------------------------------------------------------------------


def test_fixed_one_is_hold_and_fixed_zero_is_cash() -> None:
    span = _synthetic(3)
    hold = sd.levered_run(span, [1.0] * len(span.dates), 1.0, lt.MAIN)
    assert sd.fixed_run(span, 1.0).returns == hold.returns
    cash = sd.fixed_run(span, 0.0)
    assert cash.returns == pytest.approx(span.cash[span.start + 1 :], rel=1e-12)
    expected = np.asarray(span.close[span.start + 1 :]) / np.asarray(span.close[span.start : -1])
    assert np.asarray(hold.returns) + 1.0 == pytest.approx(expected, rel=1e-12)


def test_fixed_drawdown_deepens_with_f() -> None:
    span = _synthetic(5)
    drawdowns = [sd.measure(span, sd.fixed_run(span, f))["max_drawdown"] for f in COARSE_F]
    assert all(b <= a for a, b in zip(drawdowns, drawdowns[1:], strict=False))


def test_f0001_runs_through_simulate_levered_from_the_common_start() -> None:
    span = _synthetic(2)
    run = sd.levered_run(span, span.f0001, 1.0, lt.MAIN)
    frame = sd.span_frame(span)
    expected = lt.simulate_levered(
        frame,
        pd.Series(span.f0001, index=frame.index),
        start=span.dates[span.start],
        end=span.dates[-1],
        leverage=1.0,
        costs=lt.MAIN,
    ).simulation.daily
    assert run.returns == pytest.approx(tuple(expected["return"]), rel=1e-12)
    assert run.exposure == pytest.approx(tuple(expected["exposure"]), rel=1e-12)


def test_the_measures() -> None:
    span = _span([100.0, 110.0, 99.0, 105.0, 120.0, 118.0], start=0)
    run = sd.Run((0.1, -0.1, 0.06, 0.1, -0.02), (1.0, 0.5, 0.0, 1.0, 1.0), 3, None)
    m = sd.measure(span, run)
    values = np.cumprod([1.0, 1.1, 0.9, 1.06, 1.1, 0.98])
    years = (span.dates[-1] - span.dates[0]).days / 365.25
    assert m["total"] == pytest.approx(values[-1] - 1)
    assert m["cagr"] == pytest.approx(values[-1] ** (1 / years) - 1)
    assert m["max_drawdown"] == pytest.approx(-0.1)
    assert (m["drawdown_peak"], m["drawdown_trough"]) == (
        span.dates[1].isoformat(),
        span.dates[2].isoformat(),
    )
    assert m["volatility"] == pytest.approx(
        float(np.std([0.1, -0.1, 0.06, 0.1, -0.02], ddof=1)) * math.sqrt(252)
    )
    assert m["exposure"] == pytest.approx(0.7)
    assert m["in_market"] == pytest.approx(0.8)
    assert m["trades"] == 3


# R1 --------------------------------------------------------------------------------------------


def test_the_bracket_is_the_largest_shallow_enough_point() -> None:
    grid = (0.0, 0.1, 0.2, 0.3)
    dds = (0.0, -0.1, -0.2, -0.3)
    found = sd.bracket(-0.15, grid, dds)
    assert (found.low, found.monotone) == (1, True)
    assert found.weight == pytest.approx(0.5)
    assert sd.bracket(-0.2, grid, dds) == sd.Bracket(2, 0.0, True)
    assert sd.bracket(-0.4, grid, dds) == sd.Bracket(3, 0.0, True)
    assert sd.bracket(0.01, grid, dds).low is None
    assert sd.bracket(-0.15, grid, (0.0, -0.1, -0.05, -0.3)).monotone is False
    assert sd.between(sd.Bracket(1, 0.25, True), (1.0, 2.0, 4.0)) == pytest.approx(2.5)
    assert sd.between(sd.Bracket(None, 0.0, True), (1.0,)) is None


def test_r1_matches_a_shallower_line_with_fixed_f() -> None:
    fixed = [_m(-0.4 * f, 0.02 + 0.1 * f) for f in COARSE_F]
    hold = _m(-0.4, 0.12)
    line = _m(-0.23, 0.15)
    r1 = sd.same_drawdown(line, hold, COARSE_F, fixed, (), ())
    assert r1["kind"] == "fixed"
    assert (r1["low"], r1["high"]) == (0.5, 0.6)
    assert r1["at"] == pytest.approx(0.575)
    assert r1["matched_cagr"] == pytest.approx(0.02 + 0.1 * 0.575)
    assert r1["d1"] == pytest.approx(0.15 - 0.0775)
    assert r1["reference_d1"] is None
    worse = sd.same_drawdown(_m(-0.23, 0.05), hold, COARSE_F, fixed, (), ())
    assert worse["d1"] == pytest.approx(0.05 - 0.0775)


def test_r1_at_holds_drawdown_is_f_one() -> None:
    fixed = [_m(-0.4 * f, 0.02 + 0.1 * f) for f in COARSE_F]
    r1 = sd.same_drawdown(_m(-0.4, 0.10), _m(-0.4, 0.12), COARSE_F, fixed, (), ())
    assert r1["kind"] == "fixed" and r1["at"] == 1.0
    assert r1["d1"] == pytest.approx(-0.02)


def test_r1_delevers_a_deeper_line_to_holds_drawdown() -> None:
    under = tuple(round(i / 10, 1) for i in range(1, 11))
    delevered = [_m(-0.5 * x, 0.2 * x) for x in under]
    hold = _m(-0.4, 0.12)
    line = _m(-0.5, 0.2)
    r1 = sd.same_drawdown(line, hold, COARSE_F, [], under, delevered)
    assert r1["kind"] == "delever"
    assert (r1["low"], r1["high"]) == (0.8, 0.9)
    assert r1["at"] == pytest.approx(0.8)
    assert r1["matched_cagr"] == pytest.approx(0.16)
    assert r1["d1"] == pytest.approx(0.04)
    assert r1["reference_d1"] == pytest.approx(0.08)


def test_d0_and_its_opposite_sign_flag() -> None:
    span = _synthetic(4)
    out = sd.span_table(span, f_grid=COARSE_F, l_grid=COARSE_L).table
    for line in sd.RULED:
        d0 = out["d0"][line]
        exposure = out["lines"][line]["exposure"]
        assert d0["exposure"] == pytest.approx(exposure)
        base = sd.measure(span, sd.fixed_run(span, exposure))["cagr"]
        assert d0["d0"] == pytest.approx(out["lines"][line]["cagr"] - base)
        assert d0["opposite"] == (d0["d0"] * out["r1"][line]["d1"] < 0)


# R2 --------------------------------------------------------------------------------------------


def test_r2_takes_the_largest_leverage_at_holds_drawdown_and_volatility() -> None:
    grid = COARSE_L
    main = [_m(-0.2 * x, 0.1 * x, 0.1 * x) for x in grid]
    robust = [_m(-0.2 * x, 0.09 * x, 0.1 * x) for x in grid]
    hold = _m(-0.35, 0.12, 0.25)
    r1 = {"kind": "fixed", "at": 0.5, "d1": 0.01}
    cells = sd.levered_cells(r1, hold, grid, main, robust)
    assert cells["drawdown"]["leverage"] == 1.7
    assert cells["drawdown"]["main"] == pytest.approx(0.17 - 0.12)
    assert cells["drawdown"]["robust"] == pytest.approx(0.09 * 1.7 - 0.12)
    assert cells["drawdown"]["flag"] == "ok" and cells["drawdown"]["from_r1"] is False
    assert cells["volatility"]["leverage"] == 2.5
    assert cells["volatility"]["main"] == pytest.approx(0.25 - 0.12)


def test_r2_at_the_top_is_flagged() -> None:
    grid = COARSE_L
    main = [_m(-0.05 * x, 0.1 * x, 0.05 * x) for x in grid]
    cells = sd.levered_cells({"kind": "fixed"}, _m(-0.35, 0.12, 0.25), grid, main, main)
    assert cells["drawdown"]["leverage"] == 3.0
    assert cells["drawdown"]["flag"] == "top"
    assert cells["volatility"]["flag"] == "top"


def test_r2_below_one_is_the_r1_number() -> None:
    grid = COARSE_L
    main = [_m(-0.5 * x, 0.2 * x, 0.3 * x) for x in grid]
    robust = [_m(-0.5 * x, 0.18 * x, 0.3 * x) for x in grid]
    hold = _m(-0.42, 0.12, 0.25)
    under = tuple(x for x in grid if x <= 1.0)
    r1 = sd.same_drawdown(
        _m(-0.5, 0.2), hold, COARSE_F, [], under, [main[grid.index(x)] for x in under]
    )
    cells = sd.levered_cells(r1, hold, grid, main, robust)
    assert cells["drawdown"]["from_r1"] is True
    assert cells["drawdown"]["main"] == r1["d1"]
    assert cells["drawdown"]["leverage"] == r1["at"]
    assert cells["drawdown"]["robust"] == pytest.approx(0.18 * r1["at"] - 0.12)


def test_the_span_table_wires_r1_and_r2_from_the_same_runs() -> None:
    span = _synthetic(6)
    out = sd.span_table(span, f_grid=COARSE_F, l_grid=COARSE_L)
    for line in sd.RULED:
        assert out.delevered[line] == [
            sd.line_run(span, line, out.flows, x, lt.MAIN) for x in COARSE_L if x <= 1.0
        ]
        one = sd.line_run(span, line, out.flows, 1.0, lt.MAIN)
        assert out.runs[line] == one
    assert out.fixed == [sd.fixed_run(span, f) for f in COARSE_F]


# Bear segments, years and the joined row -------------------------------------------------------


@pytest.mark.parametrize("seed", [1, 6])
def test_a_shallower_line_compares_with_its_fixed_run_in_the_bear_rows(seed: int) -> None:
    span = _synthetic(seed)
    out = sd.span_table(span, f_grid=COARSE_F, l_grid=COARSE_L)
    _, inside = sd.bear_months(span, out.runs["hold"])
    for line in sd.RULED:
        r1 = out.table["r1"][line]
        assert r1["kind"] == "fixed"
        pair = (out.runs[line], sd.fixed_run(span, r1["at"]))
        expected = sd.bear_row(out.runs[line], out.runs["hold"], pair, inside)
        assert out.table["bear"]["rows"][line] == expected


@pytest.mark.parametrize("low", [0.5, None])
def test_a_delevered_line_compares_its_lower_grid_run_with_hold(
    monkeypatch: pytest.MonkeyPatch, low: float | None
) -> None:
    span = _synthetic(1)
    real = sd.same_drawdown

    def forced(*args: Any) -> dict[str, Any]:
        found = real(*args)
        return {**found, "kind": "delever", "low": low, "high": 0.6, "at": 0.55}

    monkeypatch.setattr(sd, "same_drawdown", forced)
    out = sd.span_table(span, f_grid=COARSE_F, l_grid=COARSE_L)
    _, inside = sd.bear_months(span, out.runs["hold"])
    for line in sd.RULED:
        if low is None:
            pair = None
        else:
            scaled = sd.line_run(span, line, out.flows, low, lt.MAIN)
            assert scaled == out.delevered[line][[x for x in COARSE_L if x <= 1.0].index(low)]
            pair = (scaled, out.runs["hold"])
        expected = sd.bear_row(out.runs[line], out.runs["hold"], pair, inside)
        assert out.table["bear"]["rows"][line] == expected
        if low is None:
            assert expected["inside_vs_matched"] is None


def test_bear_segments_are_holds_drop_segments() -> None:
    span = _synthetic(1)
    out = sd.span_table(span, f_grid=COARSE_F, l_grid=COARSE_L)
    hold = out.runs["hold"]
    index = pd.DatetimeIndex([pd.Timestamp(d) for d in span.dates[span.start + 1 :]])
    expected = [
        s for s in sg.segments(pd.Series(hold.returns, index=index)) if s.kind != "long_bull"
    ]
    found = out.table["bear"]["segments"]
    assert [s["id"] for s in found] == [s.id for s in expected]
    assert found
    segments, inside = sd.bear_months(span, hold)
    months = index.to_period("M")
    for flag, month in zip(inside, months, strict=True):
        assert flag == any(
            pd.Period(s["start"], freq="M") <= month <= pd.Period(s["end"], freq="M")
            for s in segments
        )


def test_bear_months_leave_out_long_bull_segments(monkeypatch: pytest.MonkeyPatch) -> None:
    span = _span([100.0] * 70, first_day="2020-01-01", start=2)
    hold = sd.Run((0.0,) * 67, (), 0, None)
    fake = [
        sg.Segment("S1", "long_bull", "2020-01", "2020-01", None, None, 1, 0.3),
        sg.Segment("S2", "bear", "2020-02", "2020-02", None, None, 1, -0.25),
        sg.Segment("S3", "long_bull", "2020-03", "2020-04", None, None, 2, 0.4),
    ]
    monkeypatch.setattr(sg, "segments", lambda returns: fake)
    segments, inside = sd.bear_months(span, hold)
    assert [s["id"] for s in segments] == ["S2"]
    days = span.dates[span.start + 1 :]
    assert inside == [d.month == 2 for d in days]


def test_a_bear_row_annualizes_on_252_days_and_compares() -> None:
    line = sd.Run((0.01, -0.02, 0.03, 0.0), (), 0, None)
    hold = sd.Run((0.02, -0.05, 0.01, 0.01), (), 0, None)
    fixed = sd.Run((0.0, -0.01, 0.0, 0.005), (), 0, None)
    row = sd.bear_row(line, hold, (line, fixed), [False, True, True, False])
    inside = (0.98 * 1.03) ** (252 / 2) - 1
    outside = 1.01 ** (252 / 2) - 1
    assert row["inside"] == pytest.approx(inside)
    assert row["outside"] == pytest.approx(outside)
    assert row["inside_vs_hold"] == pytest.approx(inside - ((0.95 * 1.01) ** 126 - 1))
    assert row["outside_vs_matched"] == pytest.approx(outside - (1.005**126 - 1))
    assert row["inside_vs_matched"] == pytest.approx(inside - (0.99 ** (252 / 2) - 1))
    delevered = sd.bear_row(line, hold, (fixed, hold), [False, True, True, False])
    assert delevered["outside_vs_matched"] == pytest.approx(
        (1.005**126 - 1) - ((1.02 * 1.01) ** 126 - 1)
    )
    assert delevered["inside"] == row["inside"]
    empty = sd.bear_row(line, hold, None, [False, True, True, False])
    assert empty["inside_vs_matched"] is None and empty["outside_vs_matched"] is None
    assert empty["inside_vs_hold"] == row["inside_vs_hold"]


def test_years_are_lines_minus_hold_from_the_common_start() -> None:
    days = list(pd.bdate_range("2019-12-27", periods=6))
    span = sd.Span(
        "qqq",
        tuple(d.date() for d in days),
        (100.0,) * 6,
        (math.nan,) + (0.0,) * 5,
        (0.5,) * 6,
        0,
        1,
    )
    runs = {
        "hold": sd.Run((0.1, 0.2, -0.1, 0.05), (), 0, None),
        "f0001": sd.Run((0.0, 0.1, 0.0, 0.0), (), 0, None),
        "x2_s5_under": sd.Run((0.1, 0.2, -0.1, 0.05), (), 0, None),
        "x2_s5_under_ladder": sd.Run((0.0, 0.0, 0.0, 0.0), (), 0, None),
    }
    rows = sd.year_rows(span, runs)
    assert [r["year"] for r in rows] == [2019, 2020]
    assert rows[0]["hold"] == pytest.approx(0.1)
    assert rows[1]["hold"] == pytest.approx(1.2 * 0.9 * 1.05 - 1)
    assert rows[0]["f0001"] == pytest.approx(-0.1)
    assert rows[1]["x2_s5_under"] == pytest.approx(0.0)
    assert rows[1]["x2_s5_under_ladder"] == pytest.approx(-(1.2 * 0.9 * 1.05 - 1))


def test_runs_join_end_to_end_with_summed_years() -> None:
    a = sd.Run((0.1, -0.5), (), 0, None)
    b = sd.Run((0.2, 0.1), (), 0, None)
    found = sd.joined([(a, 2.0), (b, 3.0)])
    total = 1.1 * 0.5 * 1.2 * 1.1
    assert found["cagr"] == pytest.approx(total ** (1 / 5.0) - 1)
    assert found["max_drawdown"] == pytest.approx(-0.5)
    assert found["years"] == 5.0


def test_the_full_row_finds_r1_on_the_joined_series() -> None:
    done = [
        sd.span_table(_synthetic(seed, start=start), f_grid=COARSE_F, l_grid=COARSE_L)
        for seed, start in ((1, 450), (2, 500), (3, 450))
    ]
    full = sd.full_row(done, COARSE_F, COARSE_L)
    years = [s.table["years"] for s in done]
    assert full["years"] == pytest.approx(sum(years))
    hold = sd.joined([(s.runs["hold"], y) for s, y in zip(done, years, strict=True)])
    assert full["lines"]["hold"] == hold
    fixed = [
        sd.joined([(s.fixed[i], y) for s, y in zip(done, years, strict=True)]) for i in range(11)
    ]
    for line in sd.RULED:
        own = sd.joined([(s.runs[line], y) for s, y in zip(done, years, strict=True)])
        under = [x for x in COARSE_L if x <= 1.0]
        delevered = [
            sd.joined([(s.delevered[line][i], y) for s, y in zip(done, years, strict=True)])
            for i in range(len(under))
        ]
        assert full["r1"][line] == sd.same_drawdown(own, hold, COARSE_F, fixed, under, delevered)


# Summary and pick --------------------------------------------------------------------------


def _row(line: str, d1: dict[str, float | None]) -> dict[str, Any]:
    return {"line": line, "d1": d1}


@pytest.mark.parametrize(
    ("under", "ladder", "chosen"),
    [
        ((-0.076, 0.01, 0.06), (-0.070, -0.02, 0.07), "x2_s5_under_ladder"),
        ((-0.070, 0.01, 0.06), (-0.0695, -0.02, 0.07), "x2_s5_under"),
        ((-0.060, 0.01, 0.06), (-0.070, 0.02, 0.07), "x2_s5_under"),
        ((-0.070, 0.01, 0.06), (-0.069, 0.02, 0.07), "x2_s5_under_ladder"),
    ],
)
def test_the_pick_rule(under: tuple[float, ...], ladder: tuple[float, ...], chosen: str) -> None:
    rows = [
        _row("f0001", dict(zip(sd.SPANS, (5.0, 5.0, 5.0), strict=True))),
        _row("x2_s5_under", dict(zip(sd.SPANS, under, strict=True))),
        _row("x2_s5_under_ladder", dict(zip(sd.SPANS, ladder, strict=True))),
    ]
    found = sd.pick(rows)
    assert found["pick"] == chosen
    assert found["lows"] == {"x2_s5_under": min(under), "x2_s5_under_ladder": min(ladder)}
    assert found["difference"] == pytest.approx(min(ladder) - min(under))
    assert found["card"] == "always"


def test_no_pick_without_every_span() -> None:
    rows = [
        _row("x2_s5_under", {"ndx": None, "dev": 0.01, "qqq": 0.02}),
        _row("x2_s5_under_ladder", {"ndx": 0.0, "dev": 0.01, "qqq": 0.02}),
    ]
    assert sd.pick(rows)["pick"] is None


def test_the_summary_rows() -> None:
    def r1(d1: float, kind: str = "fixed") -> dict[str, Any]:
        return {"d1": d1, "kind": kind}

    tables = {
        name: {
            "r1": {
                line: r1(v + i / 100, "delever" if name == "qqq" else "fixed")
                for i, line in enumerate(sd.RULED)
            },
            "r2": {line: {"drawdown": {"main": v}} for line in sd.RULED},
        }
        for name, v in (("ndx", -0.01), ("dev", 0.06), ("qqq", -0.07))
    }
    full = {"r1": {line: {"d1": 0.05} for line in sd.RULED}}
    rows = sd.summary_rows(tables, full)
    assert [r["line"] for r in rows] == list(sd.RULED)
    assert rows[1]["d1"] == {
        "ndx": pytest.approx(0.0),
        "dev": pytest.approx(0.07),
        "qqq": pytest.approx(-0.06),
    }
    assert rows[1]["kind"]["qqq"] == "delever"
    assert rows[1]["worst_span"] == "qqq"
    assert rows[2]["full_d1"] == 0.05


# Reproduction and gating -------------------------------------------------------------------


def test_the_dev_reproduction_is_the_126_path(monkeypatch: pytest.MonkeyPatch) -> None:
    span = _synthetic(7)
    found = sd.dev_reproduction(span)
    series = ec.Closes(span.dates, span.close, 0)
    block = sl.span_result(series, span.first_in)["span"]
    for line in ("x2_s5_under", "x2_s5_under_ladder", "hold"):
        row = block["strategies"][line]
        assert found["rows"][line] == {
            k: round(row[k], 6) for k in ("total", "cagr", "max_drawdown")
        }
    assert found["first_day"] == block["first_day"]
    assert found["sessions"] == block["sessions"]
    assert found["ok"] is False
    monkeypatch.setattr(sd, "DEV_PRINTED", found["rows"])
    monkeypatch.setattr(sd, "DEV_FIRST_DAY", found["first_day"])
    monkeypatch.setattr(sd, "DEV_SESSIONS", found["sessions"])
    assert sd.dev_reproduction(span)["ok"] is True
    monkeypatch.setattr(sd, "DEV_SESSIONS", found["sessions"] + 1)
    assert sd.dev_reproduction(span)["ok"] is False


@pytest.mark.parametrize("name", ["ndx", "qqq"])
def test_the_read_reproduction_is_the_122_and_126_path(name: str) -> None:
    span = _synthetic(8)._replace(name=name)
    lines = list(PRINTED_READ[name]["rows"])
    found = sd.read_reproduction(span, PRINTED_READ[name])
    series = ec.Closes(span.dates, span.close, 0)
    block = sl.span_result(series, span.first_in)["span"]
    assert list(found["rows"]) == lines
    for line in lines:
        row = block["strategies"][line]
        assert found["rows"][line] == {
            k: round(row[k], 6) for k in ("total", "cagr", "max_drawdown")
        }
    assert found["first_day"] == block["first_day"]
    assert found["last_day"] == block["last_day"] == span.dates[-1].isoformat()
    assert found["sessions"] == block["sessions"]
    assert found["ok"] is False
    pins = {key: found[key] for key in ("first_day", "last_day", "sessions", "rows")}
    assert sd.read_reproduction(span, pins)["ok"] is True
    for key, value in (
        ("first_day", "1999-01-04"),
        ("last_day", "2026-10-13"),
        ("sessions", found["sessions"] + 1),
        ("rows", {**found["rows"], lines[0]: {**found["rows"][lines[0]], "total": 0.0}}),
    ):
        assert sd.read_reproduction(span, {**pins, key: value})["ok"] is False, key


def _bench(days: pd.DatetimeIndex, seed: int = 1) -> BenchData:
    rng = np.random.default_rng(seed)
    q = rng.normal(0.0004, 0.012, len(days))
    returns = pd.DataFrame({"qqq": q, "cash": 0.0001}, index=days)
    inputs = pd.DataFrame({"ndx_return": q}, index=days)
    return BenchData(returns=returns, inputs=inputs, source={"snapshot_id": en.SNAPSHOT_ID})


def test_the_f0001_reproduction_uses_the_leverage_table_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = pd.bdate_range("1998-06-01", "2011-06-30")
    data = _bench(days)
    data.returns.loc[days > pd.Timestamp("2010-12-31"), "qqq"] = math.nan
    monkeypatch.setattr(lt, "f0001_targets", lambda d, root: pd.Series(0.5, index=d.inputs.index))
    found = sd.f0001_reproduction(data, LOADED, PROJECT_ROOT)
    cut = data.returns.loc[days <= pd.Timestamp("2010-12-31")]
    sims = {
        name: lt.simulate_levered(
            cut,
            pd.Series(target, index=cut.index),
            start=date(1999, 3, 10),
            end=date(2010, 12, 31),
            leverage=1.0,
            costs=lt.MAIN,
        ).simulation
        for name, target in (("f0001", 0.5), ("hold", 1.0))
    }
    expected = lt.reproduce_rows(
        "qqq",
        summarize(sims["f0001"], cut["cash"], LOADED),
        summarize(sims["hold"], cut["cash"], LOADED),
    )
    assert found["rows"] == expected
    assert found["ok"] is False
    assert len(found["rows"]) == 8


def _inputs(windows: dict[str, Any] | None = None) -> sd.Inputs:
    return sd.Inputs(
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        LOADED,
        {},
        pd.DataFrame(),
        {},
        windows or sd.registered_windows(LOADED),
        PROJECT_ROOT,
    )


def test_a_failed_check_reads_neither_window(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sd, "check_table", lambda inputs: {"ok": False})

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("read a window")

    monkeypatch.setattr(sd, "build_spans", boom)
    monkeypatch.setattr(sd, "span_table", boom)
    result = sd.run_table(_inputs())
    assert result["complete"] is False
    assert result["check"] == {"ok": False}
    for key in ("spans", "window_reads", "summary", "pick", "full"):
        assert key not in result


def test_a_passed_check_reads_each_window_once(monkeypatch: pytest.MonkeyPatch) -> None:
    spans = {
        name: _synthetic(seed, start=start)._replace(name=name)
        for name, seed, start in (("ndx", 1, 450), ("dev", 2, 500), ("qqq", 3, 450))
    }
    monkeypatch.setattr(sd, "check_table", lambda inputs: {"ok": True})
    monkeypatch.setattr(sd, "build_spans", lambda inputs: (spans, {"ok": True}))
    seen: list[str] = []

    def reproduced(span: sd.Span, printed: dict[str, Any]) -> dict[str, Any]:
        seen.append(span.name)
        assert printed == PRINTED_READ[span.name]
        return {"first_day": "a", "last_day": "b", "sessions": 3, "rows": {"x": {}}, "ok": True}

    monkeypatch.setattr(sd, "read_reproduction", reproduced)
    result = sd.run_table(_inputs(), f_grid=COARSE_F, l_grid=COARSE_L)
    assert seen == ["ndx", "qqq"]
    assert result["complete"] is True
    assert set(result["read_reproduction"]) == {"ndx", "qqq"}
    reads = result["window_reads"]
    assert reads["ndx"]["window_id"] == "ndx_price_1986_1999"
    assert reads["ndx"]["columns"] == ["ndx_price", "cash_return"]
    assert reads["qqq"]["columns"] == ["etf_reference_return", "cash_return", "ndx_return"]
    assert reads["ndx"]["reads"] == reads["qqq"]["reads"] == 1
    assert reads["ndx"]["owner_decision"] == sd.OWNER_DECISION
    assert result["rule"] == sd.rule_block()
    assert list(result["spans"]) == list(sd.SPANS)
    assert result["pick"]["pick"] in sd.SHIFT_LINES
    json.dumps(result, allow_nan=False)
    text = sd.render_markdown(result)
    for words in (
        "只印不判",
        "## 总表",
        "挑哪一条",
        "全程一行",
        "读取记录",
        "ndx_price_1986_1999",
        "对同回撤的对照",
        "读后先按现金 0 复现 TRADING-2565-122、-126",
    ):
        assert words in text, words
    assert text.index("## 总表") < text.index("## 挑哪一条") < text.index("## 全程一行")


@pytest.mark.parametrize("failed", ["ndx", "qqq"])
def test_a_failed_read_reproduction_prints_no_table(
    monkeypatch: pytest.MonkeyPatch, failed: str
) -> None:
    spans = {name: _synthetic(1)._replace(name=name) for name in sd.SPANS}
    monkeypatch.setattr(sd, "check_table", lambda inputs: {"ok": True})
    monkeypatch.setattr(sd, "build_spans", lambda inputs: (spans, {"ok": True}))
    monkeypatch.setattr(sd, "read_reproduction", lambda span, printed: {"ok": span.name != failed})

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("printed a table")

    monkeypatch.setattr(sd, "span_table", boom)
    result = sd.run_table(_inputs())
    assert result["complete"] is False
    assert result["read_reproduction"][failed] == {"ok": False}
    assert result["window_reads"]["ndx"]["reads"] == result["window_reads"]["qqq"]["reads"] == 1
    for key in ("spans", "summary", "pick", "full"):
        assert key not in result


def test_the_check_reports_inputs_and_both_reproductions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sd, "build_spans", lambda inputs: ({"dev": "D"}, {"ok": True}))
    monkeypatch.setattr(sd, "dev_reproduction", lambda span: {"ok": span == "D"})
    monkeypatch.setattr(sd, "f0001_reproduction", lambda data, loaded, root: {"ok": False})
    inputs = _inputs()._replace(data=_bench(pd.bdate_range("2010-01-01", periods=3)))
    check = sd.check_table(inputs)
    assert check["ok"] is False
    assert check["reproduction"] == {"dev": {"ok": True}, "f0001": {"ok": False}}
    assert check["windows"]["ndx"]["columns"] == ["ndx_price", "cash_return"]


def _closes(first: str, last: str) -> ec.Closes:
    days = pd.bdate_range(first, last)
    return ec.Closes(
        tuple(d.date() for d in days), tuple(100.0 + 0.1 * i for i in range(len(days))), 0
    )


def _stubbed_inputs(
    monkeypatch: pytest.MonkeyPatch, *, cash_gap: str | None = None, f0001_gap: str | None = None
) -> sd.Inputs:
    ndx, dev, qqq = (
        _closes("1986-10-01", "1987-03-31"),
        _closes("1999-10-01", "2000-02-29"),
        _closes("2010-10-01", "2011-02-28"),
    )
    days = pd.DatetimeIndex([pd.Timestamp(d) for d in dev.dates + qqq.dates])
    data = _bench(days)
    if cash_gap is not None:
        data.returns.loc[pd.Timestamp(cash_gap), "cash"] = math.nan
    targets = pd.Series(
        [math.nan if f0001_gap == d.date().isoformat() else 0.5 for d in days], index=days
    )
    nd = pd.DatetimeIndex([pd.Timestamp(d) for d in ndx.dates])
    frame = pd.DataFrame({"ndx_price": ndx.close, "cash_return": 0.0002}, index=nd)
    q = sp.QqqInputs(dev, 0, qqq, 0, {"ok": True})
    monkeypatch.setattr(sp, "qqq_inputs", lambda data, loaded, gate, window: q)
    monkeypatch.setattr(sp, "ndx_inputs", lambda levels, window: sp.NdxInputs(ndx, {"ok": True}))
    monkeypatch.setattr(lt, "f0001_targets", lambda data, root: targets)
    monkeypatch.setattr(sd, "ndx_targets", lambda frame, root: pd.Series(0.25, index=frame.index))
    return _inputs()._replace(data=data, ndx_frame=frame)


def test_the_spans_take_their_own_cash_and_f0001(monkeypatch: pytest.MonkeyPatch) -> None:
    spans, checks = sd.build_spans(_stubbed_inputs(monkeypatch))
    assert checks["ok"] is True
    assert set(spans["ndx"].f0001) == {0.25}
    assert set(spans["dev"].f0001) == set(spans["qqq"].f0001) == {0.5}
    assert spans["ndx"].cash[1:] == pytest.approx([0.0002] * (len(spans["ndx"].cash) - 1))
    assert spans["qqq"].cash[1:] == pytest.approx([0.0001] * (len(spans["qqq"].cash) - 1))
    assert {n: c["start"] for n, c in checks["spans"].items()} == {
        "ndx": "1987-01-02",
        "dev": "1999-12-23",
        "qqq": "2011-01-03",
    }


@pytest.mark.parametrize(
    ("gaps", "span", "key"),
    [
        ({"cash_gap": "2011-01-10"}, "qqq", "cash"),
        ({"cash_gap": "1999-12-28"}, "dev", "cash"),
        ({"f0001_gap": "2010-12-31"}, "qqq", "f0001"),
        ({"f0001_gap": "1999-11-30"}, "dev", "f0001"),
    ],
)
def test_a_missing_cash_or_opening_signal_fails_the_inputs(
    monkeypatch: pytest.MonkeyPatch, gaps: dict[str, str], span: str, key: str
) -> None:
    _, checks = sd.build_spans(_stubbed_inputs(monkeypatch, **gaps))
    assert checks["ok"] is False
    found = checks["spans"][span]
    if key == "cash":
        assert found["cash_missing"] == 1
        assert found["f0001"]["defined"] is True
    else:
        assert found["cash_missing"] == 0
        assert found["f0001"]["defined"] is False


def test_cash_is_needed_from_the_lines_own_start_before_the_common_start() -> None:
    span = _span([100.0 + i for i in range(230)], first_in=0, start=225, first_day="2020-01-01")
    gap = span._replace(cash=span.cash[:210] + (math.nan,) + span.cash[211:])
    assert sd.cash_missing(gap) == 1
    early = span._replace(cash=span.cash[:200] + (math.nan,) + span.cash[201:])
    assert sd.cash_missing(early) == 0


def test_the_input_check_needs_cash_and_f0001_at_each_start() -> None:
    span = _span([100.0 + i for i in range(30)], first_in=0, start=25, first_day="2020-01-01")
    assert sd.cash_missing(span) == 0
    # The lines start at the 200th close at the earliest, so here the check starts after 25.
    early = span._replace(cash=span.cash[:25] + (math.nan,) + span.cash[26:])
    assert sd.cash_missing(early) == 0
    gap = span._replace(cash=span.cash[:27] + (math.nan,) + span.cash[28:])
    assert sd.cash_missing(gap) == 1
    found = sd.opening_defined(span)
    assert found == {"opening": "2020-01-31", "defined": True}
    missing = span._replace(f0001=tuple(math.nan if i == 22 else 0.5 for i in range(30)))
    assert sd.opening_defined(missing)["defined"] is False


# Report and CLI ----------------------------------------------------------------------------


def test_the_rule_block() -> None:
    assert sd.rule_block() == {
        "spans": ["ndx", "dev", "qqq"],
        "common_start": {"ndx": "1987-01-02", "dev": "1999-12-23", "qqq": "2011-01-03"},
        "lines": ["hold", "f0001", "x2_s5_under", "x2_s5_under_ladder"],
        "cash": "snapshot_dtb3_cash_return",
        "f_grid": [0.0, 1.0, 0.01],
        "l_grid": [0.01, 3.0, 0.01],
        "main": [0.01, 4.0],
        "robust": [0.02, 25.0],
        "line_cost": 0.0005,
        "shift_reset": "line_trade_close_and_month_end",
        "pick_margin": 0.001,
        "simpler": "x2_s5_under",
    }


def test_the_report_marks_delevered_and_topped_cells() -> None:
    assert sd.d1_text({"d1": -0.0761, "kind": "delever"}) == "-7.61 点（降仓配平）"
    assert sd.d1_text({"d1": 0.06, "kind": "fixed"}) == "+6.00 点"
    assert sd.d2_text({"main": 0.01, "flag": "top"}, "main") == "+1.00 点（到顶）"
    assert sd.points(None) == "—"
    assert sd.DELEVER_TEXT == "线比买入持有深，降仓配平"


def test_the_rule_text_and_note() -> None:
    for words in (
        "只印不判",
        "不是交易建议",
        "一个数不改",
        "真检验只有前瞻账本",
        "对上才读",
        "只读 QQQ、它的指数和现金",
        "纳指 100 1986–1999 对 F0001 是第一次读",
        "加分批买回那条（-126）以前只在 QQQ 上跑过，在这段也是第一次印",
        "读了以后先复现 TRADING-2565-122、-126 在这两段印过的行，对不上就不印表",
    ):
        assert words in sd.RULE_TEXT, words
    for words in (
        "1987-01-02",
        "1999-12-23",
        "2011-01-03",
        "DTB3",
        "降仓",
        "0.01–3.00",
        "每段只来自一次下跌",
        "不含分红",
        "对同回撤的对照",
        "降仓线（R1 下格的 L）减买入持有",
        "250–251 个交易日",
        "在月底决定、下一个收盘调",
        "两种调法差一个收盘",
    ):
        assert words in sd.NOTE, words


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_check_refuses_an_existing_check_without_loading(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tmp_path.joinpath(sd.CHECK_JSON).write_text("{}", encoding="utf-8")
    monkeypatch.setattr(sd, "load_inputs", lambda root: (_ for _ in ()).throw(AssertionError))
    result = runner.invoke(sd.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert "已有检查结果，拒绝覆盖" in result.output


@pytest.mark.parametrize("name", [sd.RESULT_JSON, sd.RESULT_MD])
def test_run_refuses_a_second_read_without_loading(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    tmp_path.joinpath(name).write_text("{}", encoding="utf-8")
    monkeypatch.setattr(sd, "load_inputs", lambda root: (_ for _ in ()).throw(AssertionError))
    result = runner.invoke(sd.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert "已有结果，拒绝再读" in result.output


def test_a_load_failure_prints_its_first_line_and_writes_nothing(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(root: Path) -> sd.Inputs:
        raise ValueError("窗口 qqq_after_2010_owner_print 的登记和本表不一致\n第二行")

    monkeypatch.setattr(sd, "load_inputs", fail)
    for command in ("check", "run"):
        result = runner.invoke(sd.app, [command, "--out", str(tmp_path / command)])
        assert result.exit_code == 1
        assert result.output == (
            "same_drawdown_table: 数据载入失败："
            "窗口 qqq_after_2010_owner_print 的登记和本表不一致\n"
        )
        assert not (tmp_path / command).exists()


def test_a_failed_run_writes_only_the_json(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sd, "load_inputs", lambda root: _inputs())
    monkeypatch.setattr(sd, "check_table", lambda inputs: {"ok": False})
    result = runner.invoke(sd.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert "没有读两个窗口" in result.output
    assert tmp_path.joinpath(sd.RESULT_JSON).exists()
    assert not tmp_path.joinpath(sd.RESULT_MD).exists()


def test_a_failed_read_reproduction_writes_only_the_json(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spans = {name: _synthetic(1)._replace(name=name) for name in sd.SPANS}
    monkeypatch.setattr(sd, "load_inputs", lambda root: _inputs())
    monkeypatch.setattr(sd, "check_table", lambda inputs: {"ok": True})
    monkeypatch.setattr(sd, "build_spans", lambda inputs: (spans, {"ok": True}))
    result = runner.invoke(sd.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert "两个窗口各读了一次" in result.output
    assert "没有印表" in result.output
    written = json.loads(tmp_path.joinpath(sd.RESULT_JSON).read_text(encoding="utf-8"))
    assert written["complete"] is False
    assert written["read_reproduction"]["ndx"]["ok"] is False
    assert written["window_reads"]["qqq"]["reads"] == 1
    assert not tmp_path.joinpath(sd.RESULT_MD).exists()


def test_a_failed_check_writes_and_exits_one(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    check = {
        "ok": False,
        "inputs": {
            "spans": {
                "dev": {
                    "start": "1999-12-23",
                    "last_close": "2010-12-31",
                    "return_days": 2771,
                    "cash_missing": 0,
                    "f0001": {"defined": True},
                }
            }
        },
        "reproduction": {"dev": {"ok": False}, "f0001": {"ok": True}},
    }
    monkeypatch.setattr(sd, "load_inputs", lambda root: _inputs())
    monkeypatch.setattr(sd, "check_table", lambda inputs: check)
    result = runner.invoke(sd.app, ["check", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert "same_drawdown_table check: FAIL" in result.output
    assert json.loads(tmp_path.joinpath(sd.CHECK_JSON).read_text(encoding="utf-8")) == check


# Records ---------------------------------------------------------------------------------------


def test_the_requirement_and_flow_name_this_run() -> None:
    text = REQUIREMENTS.read_text(encoding="utf-8")
    assert sd.OWNER_DECISION in text
    assert "TRADING-2565-134" in text
    flow = SYSTEM_FLOW.read_text(encoding="utf-8")
    assert "research_probes/same_drawdown_table.py" in flow
