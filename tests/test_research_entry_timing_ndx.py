"""Acceptance tests for TRADING-2565-107: the 200-day pullback rule on the Nasdaq-100 1986–1999.

Requirement: knowledge qqq-options/auto-loop/period7/entry-timing/entry-timing-ndx-1986-1999-
requirements-2026-11-02.md (Baker 2026-11-02, owner_decision:TRADING-2565:2026-11-02:
ndx_price_1986_1999). Synthetic data only; no snapshot, no network.
"""

from __future__ import annotations

import ast
import json
import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import entry_timing_ndx as en
from ai_trading_system.research_probes import entry_timing_returns as er
from ai_trading_system.yaml_loader import safe_load_yaml_path

LOADED = load_settings()
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "entry_timing_ndx.py"
REGISTRY = PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
POLICY = PROJECT_ROOT / "config" / "research" / "primary_research_window_policy.yaml"
PROVENANCE = {"commit": "c" * 40, "branch": "main", "code_modified": False}
SOURCE = {"snapshot_id": "qlh-20261013-0b0df53cb1a2", "column": "ndx_price", "check_status": "WARN"}
GROWTH = 1.0005
DIP = 0.9
DIPS = ["1987-06-01", "1990-03-01", "1993-06-01", "1993-12-30", "1995-06-01", "1999-01-04"]
CRASH = "1990-03-12"
HOLIDAY = "1988-04-01"


# Constants and imports ------------------------------------------------------------------------


def test_the_run_reads_the_registered_window_under_bakers_decision() -> None:
    assert en.RUN_ID == "TRADING-2565-107"
    assert en.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-02:ndx_price_1986_1999"
    assert en.VARIANT == "v0_close" == ec.VARIANT
    assert en.WINDOW_ID == "ndx_price_1986_1999"
    assert en.WINDOW_ROLE == "owner_print_only"
    assert (en.START, en.END) == (date(1986, 1, 2), date(1999, 3, 9))
    assert en.SNAPSHOT_ID == "qlh-20261013-0b0df53cb1a2"
    assert en.COLUMN == "ndx_price"
    assert en.RANGE == "1986_1999"
    assert en.TITLE == "纳斯达克 100 价格指数 1986–1999"
    assert en.RESULT_JSON == "entry_timing_ndx.json"
    assert en.RESULT_MD == "entry_timing_ndx.md"


def test_the_data_checks_are_the_snapshot_specs() -> None:
    assert en.MAX_MOVE == qlh.load_spec().spec.checks.suspicious_index_daily_return_abs == 0.25
    assert en.END_SLACK_DAYS == 4


def test_the_module_reads_only_qqqs_own_index() -> None:
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
        "pandas",
        "ai_trading_system.config.PROJECT_ROOT",
        "ai_trading_system.core.provenance.git_provenance",
        "ai_trading_system.paths.join_root",
        "ai_trading_system.research_bench.settings.",
        "ai_trading_system.research_data.qqq_long_history",
        "ai_trading_system.research_probes.entry_timing_counts",
        "ai_trading_system.research_probes.entry_timing_returns",
        "ai_trading_system.yaml_loader.safe_load_yaml_path",
    )
    for name in imported:
        assert name.startswith(allowed), name
    assert "ai_trading_system.research_probes.entry_timing_counts" in imported
    assert "ai_trading_system.research_probes.entry_timing_returns" in imported
    banned = (
        "ma200_forward",
        "window_reads",
        "ledger.jsonl",
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
    )
    for word in banned:
        assert word not in text, word
    assert text.count(".write_text(") == 2


# The registered window ----------------------------------------------------------------------


def _registry() -> dict[str, Any]:
    raw = safe_load_yaml_path(REGISTRY)
    assert isinstance(raw, dict)
    windows = raw["windows"]
    assert isinstance(windows, dict)
    return windows


def test_the_window_is_registered_for_this_read_only() -> None:
    entry = _registry()["ndx_price_1986_1999"]
    assert entry["start"] == entry["requested_start"] == entry["actual_start"] == date(1986, 1, 2)
    assert entry["actual_portfolio_start"] == date(1986, 1, 2)
    assert entry["end"] == date(1999, 3, 9)
    assert entry["role"] == "owner_print_only"
    assert entry["question_types"] == []
    assert entry["data_source"] == qlh.load_spec().spec.source.source_id
    assert entry["data_spec"] == "config/research/qqq_long_history.yaml"
    assert entry["data_column"] == "ndx_price"
    assert entry["data_quality_contract"] == "qqq_long_history_snapshot_check"
    assert entry["read_policy"] == "once_per_owner_decision"
    assert entry["allowed_usage"] == ["owner_requested_print_only_read"]
    for usage in (
        "signal_development",
        "parameter_selection",
        "validation_read",
        "trial_or_screen",
        "promotion_evidence",
        "production",
        "paper_shadow",
        "broker_order",
    ):
        assert usage in entry["blocked_usage"], usage
    assert "nasdaq_100_price_index_without_dividends" in entry["caveats"]


def test_the_window_ends_the_session_before_qqqs_development_window() -> None:
    windows = _registry()
    entry = windows["ndx_price_1986_1999"]
    development = windows["qqq_long_history_development"]
    assert development["lookback_history_from"] == entry["start"]
    assert entry["end"] < development["start"]
    assert len(pd.bdate_range(entry["end"], development["start"])) == 2
    for name in ("qqq_long_history_validation", "exact_three_asset_validated"):
        assert entry["end"] < windows[name]["start"]


def test_no_question_type_reads_the_window() -> None:
    policy = safe_load_yaml_path(POLICY)
    for roles in policy["question_windows"].values():
        assert "ndx_price_1986_1999" not in roles.values()


def test_the_registered_window_is_read_from_the_bench_registry() -> None:
    assert en.registered_window(LOADED) == {
        "window_id": "ndx_price_1986_1999",
        "role": "owner_print_only",
        "start": "1986-01-02",
        "end": "1999-03-09",
    }


def _loaded_with(tmp_path: Path, windows: dict[str, dict[str, Any]]) -> LoadedSettings:
    lines = ["windows:"]
    for name, entry in windows.items():
        lines.append(f"  {name}:")
        lines += [f"    {key}: {value}" for key, value in entry.items()]
    tmp_path.joinpath("registry.yaml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    settings = LOADED.settings.model_copy(update={"window_registry": "registry.yaml"})
    return LoadedSettings(settings=settings, root=tmp_path, sha256=LOADED.sha256)


def test_a_missing_window_refuses_the_read(tmp_path: Path) -> None:
    loaded = _loaded_with(tmp_path, {"other": {"start": date(1986, 1, 2)}})
    with pytest.raises(ValueError, match="窗口登记表里没有 ndx_price_1986_1999"):
        en.registered_window(loaded)


@pytest.mark.parametrize(
    ("key", "value"),
    [("start", date(1985, 1, 31)), ("end", date(1999, 3, 10)), ("role", "development")],
)
def test_a_window_registered_otherwise_refuses_the_read(
    tmp_path: Path, key: str, value: Any
) -> None:
    entry = {"start": date(1986, 1, 2), "end": date(1999, 3, 9), "role": "owner_print_only"}
    loaded = _loaded_with(tmp_path, {"ndx_price_1986_1999": {**entry, key: value}})
    with pytest.raises(ValueError, match="窗口 ndx_price_1986_1999 的登记和模块不一致"):
        en.registered_window(loaded)


def test_a_window_registered_as_text_dates_is_the_same_window(tmp_path: Path) -> None:
    entry = {"start": "'1986-01-02'", "end": "'1999-03-09'", "role": "owner_print_only"}
    loaded = _loaded_with(tmp_path, {"ndx_price_1986_1999": entry})
    raw = safe_load_yaml_path(tmp_path / "registry.yaml")
    assert raw["windows"]["ndx_price_1986_1999"]["start"] == "1986-01-02"
    assert en.registered_window(loaded)["end"] == "1999-03-09"


# Closes from index levels -------------------------------------------------------------------


def test_levels_outside_the_window_never_reach_the_closes() -> None:
    levels = [
        (date(1985, 12, 31), 50.0),
        (date(1986, 1, 2), 100.0),
        (date(1986, 1, 3), 101.0),
        (date(1999, 3, 9), 102.0),
        (date(1999, 3, 10), 1.0),
    ]
    series, flat = en.prices(levels)
    assert series.dates == (date(1986, 1, 2), date(1986, 1, 3), date(1999, 3, 9))
    assert series.close == (100.0, 101.0, 102.0)
    assert series.missing == 0
    assert flat == ()


def test_a_level_equal_to_the_last_close_is_a_carried_over_session() -> None:
    levels = [
        (date(1988, 3, 30), 100.0),
        (date(1988, 3, 31), 101.0),
        (date(1988, 4, 1), 101.0),
        (date(1988, 4, 4), 101.0),
        (date(1988, 4, 5), 99.0),
    ]
    series, flat = en.prices(levels)
    assert series.dates == (date(1988, 3, 30), date(1988, 3, 31), date(1988, 4, 5))
    assert series.close == (100.0, 101.0, 99.0)
    assert flat == (date(1988, 4, 1), date(1988, 4, 4))


def test_missing_levels_count_only_after_the_first_close() -> None:
    levels = [
        (date(1986, 1, 2), math.nan),
        (date(1986, 1, 3), 100.0),
        (date(1986, 1, 6), math.inf),
        (date(1986, 1, 7), math.nan),
        (date(1986, 1, 8), 102.0),
    ]
    series, flat = en.prices(levels)
    assert series.dates == (date(1986, 1, 3), date(1986, 1, 8))
    assert series.missing == 2
    assert flat == ()


def test_the_largest_move_is_the_largest_in_size() -> None:
    series = ec.Closes(
        (date(1987, 10, 15), date(1987, 10, 16), date(1987, 10, 19), date(1987, 10, 20)),
        (100.0, 105.0, 85.0, 90.0),
        0,
    )
    move = en.largest_move(series)
    assert move is not None
    assert move["date"] == "1987-10-19"
    assert move["return"] == pytest.approx(85.0 / 105.0 - 1.0)
    assert en.largest_move(ec.Closes((date(1987, 1, 2),), (1.0,), 0)) is None


# The synthetic window -----------------------------------------------------------------------


INDEX = pd.bdate_range("1986-01-02", "1999-03-09", name="date")


def _pos(day: str) -> int:
    position = INDEX.get_loc(pd.Timestamp(day))
    assert isinstance(position, int)
    return position


def _path(dips: list[str] = DIPS, crash: str | None = CRASH) -> np.ndarray:
    level = 100.0 * GROWTH ** np.arange(len(INDEX), dtype=float)
    for day in dips:
        level[_pos(day)] *= DIP
    if crash is not None:
        level[_pos(crash) :] *= 0.85
    level[_pos(HOLIDAY)] = level[_pos(HOLIDAY) - 1]
    return level


def _levels(level: np.ndarray, index: pd.DatetimeIndex = INDEX) -> list[tuple[date, float]]:
    return [(day.date(), float(value)) for day, value in zip(index, level, strict=True)]


LEVELS = _levels(_path())
WINDOW = en.registered_window(LOADED)


@pytest.fixture(scope="module")
def probe() -> dict[str, Any]:
    return en.run_probe(LEVELS, SOURCE, WINDOW, provenance=PROVENANCE)


def test_the_check_counts_the_window_like_the_counts_module() -> None:
    series, flat = en.prices(LEVELS)
    check = en.check_inputs(series, flat, WINDOW)
    assert check["ok"] is True
    assert list(check) == ["run_id", "owner_decision", "variant", "rule", "ndx", "ok"]
    assert check["rule"] == {
        "sma_days": 200,
        "slope_lag": 20,
        "confirm_days": 3,
        "cooldown": 20,
        "hold_days": 60,
    }
    row = check["ndx"]
    touches = ec.scan(series.close, 0)
    entries = [t.entry for t in touches if t.counted and t.entry is not None]
    mask = ec.held(len(series.dates), entries)
    assert row["ranges"] == [ec.count_range(series, touches, mask, "1986_1999", en.START, en.END)]
    assert row["entries"] == [series.dates[e].isoformat() for e in entries]
    assert len(row["entries"]) >= 3
    assert row["window_id"] == "ndx_price_1986_1999"
    assert row["role"] == "owner_print_only"
    assert (row["start"], row["end"]) == ("1986-01-02", "1999-03-09")
    assert row["first_close_date"] == "1986-01-02"
    assert row["last_close_date"] == "1999-03-09"
    assert row["first_scan_date"] == series.dates[200].isoformat()
    assert row["missing_days"] == 0
    assert (row["flat_days"], row["flat_dates"]) == (1, [HOLIDAY])
    assert row["largest_move"]["date"] == CRASH
    assert row["largest_move"]["return"] == pytest.approx(0.85 * GROWTH - 1.0)
    assert list(row) == [
        "window_id",
        "role",
        "start",
        "end",
        "first_close_date",
        "last_close_date",
        "first_scan_date",
        "missing_days",
        "flat_days",
        "flat_dates",
        "largest_move",
        "ranges",
        "entries",
    ]


def _check(levels: list[tuple[date, float]]) -> dict[str, Any]:
    series, flat = en.prices(levels)
    return en.check_inputs(series, flat, WINDOW)


def test_a_hole_in_the_levels_fails_the_check() -> None:
    levels = list(LEVELS)
    levels[500] = (levels[500][0], math.nan)
    check = _check(levels)
    assert check["ndx"]["missing_days"] == 1
    assert check["ok"] is False


def test_levels_starting_after_the_window_start_fail_the_check() -> None:
    check = _check(LEVELS[1:])
    assert check["ndx"]["first_close_date"] == "1986-01-03"
    assert check["ok"] is False


def test_the_last_close_may_fall_four_days_short_of_the_end() -> None:
    cut = [(day, value) for day, value in LEVELS if day <= date(1999, 3, 5)]
    assert _check(cut)["ok"] is True
    cut = [(day, value) for day, value in LEVELS if day <= date(1999, 3, 4)]
    assert _check(cut)["ndx"]["last_close_date"] == "1999-03-04"
    assert _check(cut)["ok"] is False


@pytest.mark.parametrize(("jump", "ok"), [(1.24, True), (1.26, False), (0.74, False)])
def test_a_daily_move_beyond_a_quarter_fails_the_check(jump: float, ok: bool) -> None:
    level = _path(crash=None)
    level[_pos("1994-01-03") :] *= jump
    check = _check(_levels(level))
    assert check["ndx"]["largest_move"]["date"] == "1994-01-03"
    assert check["ok"] is ok


def test_too_few_closes_to_scan_fail_the_check() -> None:
    short = [(day, value) for day, value in LEVELS if day <= date(1986, 9, 1)]
    short.append((date(1999, 3, 9), short[-1][1] * 1.01))
    check = _check(short)
    assert check["ndx"]["first_scan_date"] is None
    assert check["ok"] is False


# Returns ------------------------------------------------------------------------------------


def _relabel(found: Any) -> Any:
    if isinstance(found, dict):
        return {k: _relabel(v) for k, v in found.items()}
    if isinstance(found, list):
        return [_relabel(v) for v in found]
    return "1986_1999" if found == "1999_2010" else found


def test_the_returns_are_the_returns_tables_arithmetic() -> None:
    series, _ = en.prices(LEVELS)
    found = en.ndx_returns(series, en.START, en.END)
    expected = er.market_returns(series, "qqq", en.START, en.END)
    assert found == _relabel(expected)
    assert list(found) == ["signals", "ranges"]
    assert [r["range"] for r in found["ranges"]] == ["1986_1999"]
    assert found["ranges"][0]["start"] == "1986-01-02"
    assert found["ranges"][0]["end"] == "1999-03-09"


def test_the_returns_on_a_later_start_match_too() -> None:
    series, _ = en.prices(LEVELS)
    start = date(1990, 1, 2)
    found = en.ndx_returns(series, start, en.END)
    assert found == _relabel(er.market_returns(series, "qqq", start, en.END))
    assert all(s["touch"] >= "1990-01-02" for s in found["signals"])


def test_a_december_touch_takes_the_year_of_its_january_entry(probe: dict[str, Any]) -> None:
    signals = {s["touch"]: s for s in probe["returns"]["signals"]}
    turn = signals["1993-12-30"]
    assert (turn["entry"], turn["year"]) == ("1994-01-04", 1994)
    series, _ = en.prices(LEVELS)
    for h in er.HORIZONS:
        outcomes = [er.outcome(series.close, d, h) for d in range(len(series.close))]
        year = er.baseline(series.dates, outcomes, date(1994, 1, 1), date(1994, 12, 31))
        assert turn[f"h{h}"]["year_n"] == year["n"]
        assert turn[f"h{h}"]["year_plain"] == pytest.approx(year["plain_mean"])


def test_a_signal_on_a_falling_average_is_neither_counted_nor_listed() -> None:
    close = [100.0 / GROWTH**i for i in range(600)]
    close[399] = close[398] * 3.0
    close[401] = close[402] = close[403] = close[399]
    days = tuple(d.date() for d in pd.bdate_range("1986-01-02", periods=600))
    series = ec.Closes(days, tuple(close), 0)
    touches = ec.scan(series.close, 0)
    assert [(t.outcome, t.counted) for t in touches][:1] == [("signal", False)]
    check = en.check_inputs(series, (), WINDOW)
    assert check["ndx"]["entries"] == []
    assert check["ndx"]["ranges"][0]["signals"] >= 1
    assert check["ndx"]["ranges"][0]["counted"] == 0
    found = en.ndx_returns(series, days[0], days[-1])
    assert found["signals"] == []
    assert found["ranges"][0]["signals"] == 0


def test_the_crash_stops_the_signal_before_it(probe: dict[str, Any]) -> None:
    signals = {s["touch"]: s for s in probe["returns"]["signals"]}
    crashed = signals["1990-03-01"]
    assert crashed["h20"]["stop_day"] is not None
    assert crashed["h20"]["stop"] <= -0.08
    assert signals["1987-06-01"]["h60"]["stop_day"] is None
    last = signals["1999-01-04"]
    assert last["h20"] is not None
    assert last["h60"] is None


def test_the_probe_prints_counts_and_returns_after_a_passing_check(
    probe: dict[str, Any],
) -> None:
    assert list(probe) == [
        "run_id",
        "owner_decision",
        "variant",
        "rule",
        "provenance",
        "source",
        "check",
        "returns",
        "complete",
    ]
    assert probe["run_id"] == "TRADING-2565-107"
    assert probe["owner_decision"] == en.OWNER_DECISION
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
    assert probe["source"] == SOURCE
    series, flat = en.prices(LEVELS)
    assert probe["check"] == en.check_inputs(series, flat, WINDOW)
    assert probe["returns"] == en.ndx_returns(series, en.START, en.END)
    assert probe["complete"] is True
    entries = [s["entry"] for s in probe["returns"]["signals"]]
    assert entries == probe["check"]["ndx"]["entries"]


def test_levels_after_the_window_change_nothing(probe: dict[str, Any]) -> None:
    later = pd.bdate_range("1999-03-10", "1999-06-30", name="date")
    tail = _levels(np.full(len(later), 1.0), later)
    assert en.run_probe([*LEVELS, *tail], SOURCE, WINDOW, provenance=PROVENANCE) == probe


def test_a_failed_check_computes_no_returns() -> None:
    levels = list(LEVELS)
    levels[500] = (levels[500][0], math.nan)
    probe = en.run_probe(levels, SOURCE, WINDOW, provenance=PROVENANCE)
    assert probe["complete"] is False
    assert probe["returns"] is None
    assert probe["check"]["ok"] is False


# Markdown and screen ------------------------------------------------------------------------


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{100.0 * value:+.2f}%"


def _share(value: float | None) -> str:
    return "—" if value is None else f"{100.0 * value:.1f}%"


def _row(cells: list[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def test_the_markdown_gives_the_data_the_counts_and_the_returns(probe: dict[str, Any]) -> None:
    text = en.render_markdown(probe)
    row = probe["check"]["ndx"]
    c = row["ranges"][0]
    move = row["largest_move"]
    expected = [
        "# 200 日线回踩收盘价版：纳斯达克 100 价格指数 1986–1999（TRADING-2565-107）",
        "",
        en.RULE_TEXT,
        "",
        f"数据：快照 `qlh-20261013-0b0df53cb1a2` 的 `ndx_price`，1986-01-02..1999-03-09，"
        f"{c['sessions']} 个收盘；去掉的同值日 1 天（{HOLIDAY}）；"
        f"最大单日变动 {_pct(move['return'])}（{CRASH}）。",
        "",
        "## 次数",
        "",
        "| 范围 | 交易日 | 触线 | 信号 | 失败 | 待定 | 计入 | 持有天数 | 平均敞口 "
        "| 到期末不满 60 天 |",
        "|" + "---|" * 10,
        _row(
            [
                "1986–1999",
                str(c["sessions"]),
                str(c["touches"]),
                str(c["signals"]),
                str(c["failed"]),
                str(c["pending"]),
                str(c["counted"]),
                str(c["hold_days"]),
                _share(c["exposure"]),
                str(c["open_at_end"]),
            ]
        ),
        "",
        "## 收益",
        "",
        "| 触线日 | 入场日 | 20 天 | 20 天止损 | 当年 20 天 | 当年 20 天止损 "
        "| 60 天 | 60 天止损 | 当年 60 天 | 当年 60 天止损 |",
        "|" + "---|" * 10,
    ]
    found = probe["returns"]
    for s in found["signals"]:
        cells = [s["touch"], s["entry"]]
        for h in (20, 60):
            cell = s[f"h{h}"]
            if cell is None:
                cells += ["—"] * 4
                continue
            stop = _pct(cell["stop"])
            if cell["stop_day"] is not None:
                stop += f"（第 {cell['stop_day']} 天止损）"
            cells += [_pct(cell["plain"]), stop, _pct(cell["year_plain"]), _pct(cell["year_stop"])]
        expected.append(_row(cells))
    expected += [
        "",
        "| 范围 | 天数 | 信号 | 均值 | 止损均值 | 当年均值 | 当年止损均值 | 上涨 | 止损上涨 "
        "| 赢当年 | 止损赢当年 | 止损触发 | 任一天均值 | 任一天止损均值 | 任一天上涨比例 "
        "| 任一天止损上涨比例 |",
        "|" + "---|" * 16,
    ]
    for r in found["ranges"]:
        for h in (20, 60):
            v, k = r[f"h{h}"], r[f"h{h}"]["settled"]
            cells = [r["range"].replace("_", "–"), str(h), str(k)]
            cells += [_pct(v[key]) for key in ("plain_mean", "stop_mean")]
            cells += [_pct(v[key]) for key in ("year_plain_mean", "year_stop_mean")]
            cells += [
                f"{v[key]}/{k}"
                for key in ("plain_up", "stop_up", "plain_beats_year", "stop_beats_year", "stopped")
            ]
            cells += [_pct(v["any_day_plain_mean"]), _pct(v["any_day_stop_mean"])]
            cells += [_share(v["any_day_plain_up_share"]), _share(v["any_day_stop_up_share"])]
            expected.append(_row(cells))
    assert text == "\n".join(expected) + "\n"
    assert "天止损）" in text
    assert "| — | — | — | — |" in text


def test_the_rule_text_names_the_index_and_its_caveats() -> None:
    assert en.RULE_TEXT.startswith(
        "规则：v0_close（收盘价触线、3 天确认、20 天冷却、均线高于 20 天前才计入）"
    )
    assert "h 天收益和 8% 收盘止损同 TRADING-2565-105" in en.RULE_TEXT
    assert "h 天后超出 1999-03-09 的日子不算" in en.RULE_TEXT
    assert "纳斯达克 100 价格指数（QQQ 跟踪的指数，FRED，不含分红），1986-01-02..1999-03-09" in (
        en.RULE_TEXT
    )
    assert "和前一个收盘同值的日子当作休市沿用前值，去掉" in en.RULE_TEXT
    assert en.RULE_TEXT.endswith("只印，不判。")
    assert "\n" not in en.RULE_TEXT


def test_the_markdown_says_none_when_no_session_was_carried_over() -> None:
    level = _path()
    level[_pos(HOLIDAY)] = level[_pos(HOLIDAY) - 1] * GROWTH
    probe = en.run_probe(_levels(level), SOURCE, WINDOW, provenance=PROVENANCE)
    assert "去掉的同值日 0 天（无）" in en.render_markdown(probe)


def test_the_screen_line_gives_counts_and_both_horizons(
    probe: dict[str, Any], tmp_path: Path
) -> None:
    c = probe["check"]["ndx"]["ranges"][0]
    r = probe["returns"]["ranges"][0]
    assert en.summary_line(probe, tmp_path) == (
        f"entry_timing_ndx: 纳斯达克 100 价格指数 1986–1999 触线 {c['touches']}、信号 "
        f"{c['signals']}、计入 {c['counted']}；60 天均值 {_pct(r['h60']['plain_mean'])}"
        f"（止损 {_pct(r['h60']['stop_mean'])}），当年任一天 {_pct(r['h60']['year_plain_mean'])}"
        f"（止损 {_pct(r['h60']['year_stop_mean'])}）；20 天均值 {_pct(r['h20']['plain_mean'])}"
        f"（止损 {_pct(r['h20']['stop_mean'])}），当年任一天 {_pct(r['h20']['year_plain_mean'])}"
        f"（止损 {_pct(r['h20']['year_stop_mean'])}）；结果目录 {tmp_path}"
    )


# Loading ------------------------------------------------------------------------------------


def _daily() -> pd.DataFrame:
    index = INDEX.append(pd.bdate_range("1999-03-10", "1999-04-30"))
    level = 100.0 * GROWTH ** np.arange(len(index), dtype=float)
    return pd.DataFrame({"ndx_price": level, "ndx_tr": level * 2.0}, index=index)


REPORT = {
    "snapshot_id": "qlh-20261013-0b0df53cb1a2",
    "status": "WARN",
    "series_csv_sha256": "a" * 64,
    "issues": [
        {"severity": "WARN", "code": "suspicious_index_return"},
        {"severity": "WARN", "code": "long_calendar_gap"},
        {"severity": "WARN", "code": "long_calendar_gap"},
        {"severity": "INFO", "code": "other"},
    ],
}


@pytest.fixture
def snapshot(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[tuple[Path, str | None]]:
    calls: list[tuple[Path, str | None]] = []

    def resolve(root: Path, snapshot_id: str | None = None) -> Path:
        calls.append((root, snapshot_id))
        return tmp_path / str(snapshot_id)

    def load(snapshot_dir: Path, loaded: Any = None) -> tuple[pd.DataFrame, dict[str, Any]]:
        assert snapshot_dir == tmp_path / "qlh-20261013-0b0df53cb1a2"
        return _daily(), dict(REPORT)

    monkeypatch.setattr(qlh, "resolve_snapshot_dir", resolve)
    monkeypatch.setattr(qlh, "load_daily", load)
    return calls


def test_the_load_reads_the_named_snapshot_up_to_the_window_end(
    snapshot: list[tuple[Path, str | None]], tmp_path: Path
) -> None:
    levels, source, window = en.load_data(tmp_path / "root")
    assert snapshot == [(tmp_path / "root", "qlh-20261013-0b0df53cb1a2")]
    assert levels[0][0] == date(1986, 1, 2)
    assert levels[-1][0] == date(1999, 3, 9)
    assert len(levels) == len(INDEX)
    assert levels[1][1] == pytest.approx(100.0 * GROWTH)
    assert all(isinstance(day, date) and isinstance(value, float) for day, value in levels)
    assert source == {
        "snapshot_id": "qlh-20261013-0b0df53cb1a2",
        "column": "ndx_price",
        "check_status": "WARN",
        "series_csv_sha256": "a" * 64,
        "check_warnings": ["long_calendar_gap", "suspicious_index_return"],
    }
    assert window == en.registered_window(LOADED)


def test_another_snapshot_refuses_the_load(
    monkeypatch: pytest.MonkeyPatch, snapshot: list[tuple[Path, str | None]], tmp_path: Path
) -> None:
    monkeypatch.setattr(
        qlh,
        "load_daily",
        lambda snapshot_dir, loaded=None: (_daily(), {**REPORT, "snapshot_id": "x"}),
    )
    with pytest.raises(ValueError, match="快照不是 qlh-20261013-0b0df53cb1a2"):
        en.load_data(tmp_path)


# CLI ----------------------------------------------------------------------------------------


@pytest.fixture
def loaded_cli(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    roots: list[Path] = []

    def load_data(root: Path) -> Any:
        roots.append(root)
        return LEVELS, SOURCE, WINDOW

    monkeypatch.setattr(en, "load_data", load_data)
    monkeypatch.setattr(en, "git_provenance", lambda root: PROVENANCE)
    return roots


def test_run_writes_the_json_and_the_table(
    loaded_cli: list[Path], probe: dict[str, Any], tmp_path: Path
) -> None:
    out = tmp_path / "out"
    result = CliRunner().invoke(en.app, ["run", "--out", str(out), "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [tmp_path]
    text = out.joinpath("entry_timing_ndx.json").read_text(encoding="utf-8")
    assert text == json.dumps(probe, indent=2, ensure_ascii=False) + "\n"
    table = out.joinpath("entry_timing_ndx.md").read_text(encoding="utf-8")
    assert table == en.render_markdown(probe)
    assert sorted(p.name for p in out.iterdir()) == ["entry_timing_ndx.json", "entry_timing_ndx.md"]
    assert result.output == en.summary_line(probe, out) + "\n"


def test_run_defaults_to_the_long_history_snapshot_root(
    loaded_cli: list[Path], tmp_path: Path
) -> None:
    result = CliRunner().invoke(en.app, ["run", "--out", str(tmp_path / "out")])
    assert result.exit_code == 0, result.output
    assert loaded_cli == [qlh.DEFAULT_SNAPSHOT_ROOT]


@pytest.mark.parametrize("existing", ["entry_timing_ndx.json", "entry_timing_ndx.md"])
def test_run_refuses_an_existing_result_without_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, existing: str
) -> None:
    def load_data(root: Path) -> Any:
        raise AssertionError("loaded")

    monkeypatch.setattr(en, "load_data", load_data)
    tmp_path.joinpath(existing).write_text("x", encoding="utf-8")
    result = CliRunner().invoke(en.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_timing_ndx: 已有结果，拒绝覆盖。\n"
    assert [p.name for p in tmp_path.iterdir()] == [existing]


@pytest.mark.parametrize(
    "error",
    [
        ValueError("窗口登记表里没有 ndx_price_1986_1999。\nmore"),
        KeyError("ndx_price"),
        OSError("x"),
    ],
)
def test_a_load_failure_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, error: Exception
) -> None:
    def load_data(root: Path) -> Any:
        raise error

    monkeypatch.setattr(en, "load_data", load_data)
    out = tmp_path / "out"
    result = CliRunner().invoke(en.app, ["run", "--out", str(out)])
    assert result.exit_code == 1
    first = str(error).splitlines()[0]
    assert result.output == f"entry_timing_ndx: 数据载入失败：{first}\n"
    assert not out.exists()


def test_a_failed_check_writes_only_the_json(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    levels = list(LEVELS)
    levels[500] = (levels[500][0], math.nan)
    monkeypatch.setattr(en, "load_data", lambda root: (levels, SOURCE, WINDOW))
    monkeypatch.setattr(en, "git_provenance", lambda root: PROVENANCE)
    result = CliRunner().invoke(en.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert result.output == "entry_timing_ndx: 输入检查 FAIL，没有算收益\n"
    assert [p.name for p in tmp_path.iterdir()] == ["entry_timing_ndx.json"]
    written = json.loads(tmp_path.joinpath("entry_timing_ndx.json").read_text("utf-8"))
    assert written["complete"] is False and written["returns"] is None


def test_the_window_end_is_a_day_before_qqq_trades() -> None:
    assert en.END + timedelta(days=1) == date(1999, 3, 10)
