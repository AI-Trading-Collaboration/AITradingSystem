"""Acceptance tests for TRADING-2565-135: the forward ledger's added lines (5.4a).

Requirement: knowledge qqq-options/auto-loop/period7/
forward-ledger-added-lines-requirements-2026-11-14.md v1.1 (period 7 charter sections 3.3, 3.5
and 5.4; reviewed by 「研究复盘 11-14」). F0001 x 2 and the total-asset column hang on the main
ledger's rows and never block them. Synthetic files only.
"""

from __future__ import annotations

import ast
import hashlib
import json
import math
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.research_probes import forward_added as fa
from ai_trading_system.research_probes import forward_ledger as fl
from ai_trading_system.trading_calendar import is_us_equity_trading_day

MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "forward_added.py"
LEDGER_MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "forward_ledger.py"
REQUIREMENTS = PROJECT_ROOT / "docs" / "requirements" / "TRADING-2565_Research_Bench.md"
SYSTEM_FLOW = PROJECT_ROOT / "docs" / "system_flow.md"
README = PROJECT_ROOT / "research" / "_forward" / "README.md"
RECORDS = PROJECT_ROOT / "research" / "_loop" / "forward_records.md"
COMMITTED = PROJECT_ROOT / "research" / "_forward" / fl.LEDGER_FILE
# The first 24 committed rows (2026-10-12..2026-11-12) as main 44dbd582 holds them, LF-joined.
COMMITTED_24 = "f20e12cd6dfaa44271a1a1118a68f3a3ef791ae3ea90db8c793847a23264d640"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
PROVENANCE = {"commit": COMMIT, "branch": "main", "code_modified": False}
# Synthetic runs start the added lines here (after the 11-03 reset row) and end at END.
START = date(2026, 11, 5)
END = date(2026, 12, 11)
PRICE_FROM = date(2026, 9, 1)
# Reset rows in the synthetic ledger and the main F0001 exposure they reset to.
RESETS = {date(2026, 11, 3): 0.5, date(2026, 12, 2): 1.0}
ODD = date(2026, 12, 3)  # SGOV pays out without adjustment: a negative day while borrowing
HIGH = date(2026, 12, 8)  # and a day above 0.1% while borrowing
NO_SNAPSHOT = date(2026, 11, 10)
BAD_TOTAL = date(2026, 11, 17)
NO_TOTAL = date(2026, 11, 18)
DAY = 1.0 / 252


def _qqq_return(day: date) -> float:
    return 0.0005 + 0.012 * math.sin(day.toordinal() / 5.0)


def _sgov_return(day: date) -> float:
    if day == ODD:
        return -0.0035
    if day == HIGH:
        return 0.0025
    return 0.00016 + 0.00001 * (day.toordinal() % 3)


def _series(returns: Callable[[date], float]) -> pd.Series:
    days = fl.trading_days(PRICE_FROM, date(2027, 1, 29))
    values = 100.0 * np.cumprod([1.0] + [1.0 + returns(d) for d in days[1:]])
    return pd.Series(values, index=pd.DatetimeIndex(days))


QQQ = _series(_qqq_return)
SGOV = _series(_sgov_return)
# The capture files carry ten decimals; the oracle reads SGOV's return from the same digits.
ROUNDED_SGOV = SGOV.round(10)


def _captured_sgov(day: date) -> float:
    stamp = pd.Timestamp(day)
    before = ROUNDED_SGOV[ROUNDED_SGOV.index < stamp]
    return float(ROUNDED_SGOV[stamp] / before.iloc[-1] - 1.0)


def _write_capture(
    root: Path, day: date, *, sgov: pd.Series | None = SGOV, skip: tuple[date, ...] = ()
) -> None:
    path = root / fl.PRICES_REL.format(day=day.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for stamp in QQQ.index[QQQ.index <= pd.Timestamp(day)]:
        text = stamp.date().isoformat()
        rows.append(f"{text},QQQ,1,1,1,1,{QQQ[stamp]:.10f},100")
        if sgov is not None and stamp.date() not in skip:
            rows.append(f"{text},SGOV,1,1,1,1,{sgov[stamp]:.10f},100")
    path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )


def _final(day: date) -> tuple[float, float]:
    k = day.toordinal() % 3
    return 0.2 + 0.1 * k, 0.4 + 0.1 * k


def _total(day: date) -> tuple[float, float]:
    k = day.toordinal() % 4
    return 0.3 + 0.05 * k, 0.5 + 0.05 * k


def _write_snapshot(
    root: Path,
    day: date,
    *,
    total: tuple[float, float] | None = None,
    final: tuple[float, float] | None = None,
    drop_total: bool = False,
) -> None:
    low, high = final or _final(day)
    positions: dict[str, Any] = {
        fl.BAND_KEY: {"min_position": low, "max_position": high, "label": "中性"},
        fl.MODEL_BAND_KEY: {"min_position": 0.3, "max_position": 0.5, "label": "模型"},
    }
    if not drop_total:
        t_low, t_high = total or _total(day)
        positions[fa.TOTAL_KEY] = {"min_position": t_low, "max_position": t_high, "label": "x"}
    path = root / fl.SNAPSHOT_REL.format(day=day.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"positions": positions}, ensure_ascii=False), encoding="utf-8")


@pytest.fixture()
def daily_root(tmp_path: Path) -> Path:
    root = tmp_path / "daily"
    for day in fl.trading_days(fl.HELD_FROM, END):
        _write_capture(root, day)
        if day == NO_SNAPSHOT:
            continue
        if day == BAD_TOTAL:
            _write_snapshot(root, day, total=(0.7, 0.4))
        else:
            _write_snapshot(root, day, drop_total=day == NO_TOTAL)
    return root


@pytest.fixture()
def ledger_dir(tmp_path: Path) -> Path:
    path = tmp_path / "forward"
    path.mkdir()
    return path


@pytest.fixture()
def started(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fa, "ADDED_START", START)


def _main_through(
    ledger_dir: Path,
    root: Path,
    through: date,
    *,
    qqq: dict[date, float | None] | None = None,
    resets: dict[date, float] | None = None,
) -> list[dict[str, Any]]:
    """Append valid main-ledger rows up to through (the main ledger's own rules, simplified)."""
    resets = RESETS if resets is None else resets
    existing = fl.read_ledger(ledger_dir)
    raw = fl._ledger_lines(ledger_dir)
    prev = chain_digest(raw[-1]) if raw else None
    exposure = 1.0
    for row in existing:
        exposure = row["exp_overlay"]
        q = row["qqq_tr"]
        if q is not None:
            exposure = exposure * (1.0 + q) / (1.0 + exposure * q)
    start = (
        fl.BACKFILL_START
        if not existing
        else fl._next_day(date.fromisoformat(existing[-1]["date"]))
    )
    days = fl.trading_days(fl.HELD_FROM, through)
    rows: list[dict[str, Any]] = []
    for day in fl.trading_days(start, through):
        q = (qqq or {}).get(day, _qqq_return(day))
        if day in resets:
            exposure = resets[day]
        band = fl.read_band(root, day)
        held = None
        for earlier in reversed(days[: max(0, days.index(day) - 1)]):
            found = fl.read_band(root, earlier)
            if found is not None:
                held = found.mid
                break
        row = {
            "schema_version": fl.SCHEMA,
            "seq": len(existing) + len(rows) + 1,
            "date": day.isoformat(),
            "backfill": day < fl.FORWARD_START,
            "qqq_tr": q,
            "price_file": fl.PRICES_REL.format(day=day.isoformat()),
            "price_last_date": day.isoformat(),
            "exp_daily": None if band is None else band.mid,
            "daily_min": None if band is None else band.min_position,
            "daily_max": None if band is None else band.max_position,
            "daily_label": None if band is None else band.label,
            "daily_model_mid": None if band is None else band.model_mid,
            "snapshot_file": None if band is None else band.file,
            "exp_daily_held": held,
            "exp_fixed": fl.FIXED_EXPOSURE,
            "overlay": fl.OVERLAY_ID,
            "exp_overlay": exposure,
            "ret_daily": fl._product(held, q),
            "ret_fixed": fl._product(fl.FIXED_EXPOSURE, q),
            "ret_overlay": fl._product(exposure, q),
            "notes": [],
            "code_commit": COMMIT,
            "code_modified": False,
            "prev_sha256": prev,
        }
        rows.append(row)
        prev = chain_digest(fl._row_bytes(row))
        if q is not None:
            exposure = exposure * (1.0 + q) / (1.0 + exposure * q)
    with (ledger_dir / fl.LEDGER_FILE).open("ab") as sink:
        sink.write(b"".join(fl._row_bytes(row) + b"\n" for row in rows))
    assert fl.verify(ledger_dir) == []
    return rows


def _added_bytes(ledger_dir: Path) -> bytes:
    return (ledger_dir / fa.ADDED_FILE).read_bytes()


def _rows(ledger_dir: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in _added_bytes(ledger_dir).splitlines()]


def _oracle(
    main_rows: list[dict[str, Any]],
    sgov: Callable[[date], float | None] = _captured_sgov,
    first_target: float = 1.0,
) -> list[tuple[float | None, float | None, float | None]]:
    """Independent (w, b, r) for every main row from START, by the requirement's words."""
    out: list[tuple[float | None, float | None, float | None]] = []
    w: float | None = None
    q_prev: float | None = None
    r_prev: float | None = None
    wiped = False
    for k, row in enumerate(r for r in main_rows if r["date"] >= START.isoformat()):
        day = date.fromisoformat(row["date"])
        if k == 0:
            w = first_target
        elif wiped:
            w = None
        elif day in RESETS:
            w = 2.0 * RESETS[day]
        elif w is None or w in (0.0, 1.0):
            pass
        elif q_prev is None or r_prev is None:
            w = None
        else:
            w = w * (1.0 + q_prev) / (1.0 + r_prev)
        q = row["qqq_tr"]
        if w is None:
            b, r = None, None
        else:
            b = max(w - 1.0, 0.0)
            s = sgov(day)
            if q is None or (b > 0.0 and s is None):
                r = None
            else:
                r = w * q - (b * (s + 0.01 * DAY) if b > 0.0 and s is not None else 0.0)
                if 1.0 + r <= 0.0:
                    r = -1.0
                    wiped = True
        out.append((w, b, r))
        q_prev, r_prev = q, r
    return out


# Constants and boundaries ------------------------------------------------------------------------


def test_constants_name_the_run_the_decision_and_the_file() -> None:
    assert fa.RUN_ID == "TRADING-2565-135"
    assert fa.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-01:research_loop_p7"
    assert fa.SCHEMA == "forward_added.v1"
    assert fa.ADDED_FILE == "added_lines.jsonl"
    assert fa.ADDED_START == date(2026, 11, 19)
    assert is_us_equity_trading_day(fa.ADDED_START)
    assert (fa.FACTOR, fa.SPREAD) == (2.0, 0.01)
    assert fa.TARGETS == {0.0, 1.0, 2.0}
    assert fa.LEDGER_TARGETS == {0.0, 0.5, 1.0}
    assert fa.TOTAL_KEY == "total_asset_ai_band"
    assert fa.WIPED == "净值归零"
    assert fa.SECTION == "## 加的线"
    assert fa.ROW_KEYS == {
        "schema_version",
        "seq",
        "date",
        "ledger_seq",
        "ledger_sha256",
        "qqq_tr",
        "price_file",
        "target_overlay2",
        "exp_overlay2",
        "borrow_overlay2",
        "sgov_tr",
        "ret_overlay2",
        "total_min",
        "total_max",
        "total_snapshot_file",
        "exp_total_held",
        "ret_total",
        "notes",
        "code_commit",
        "code_modified",
        "prev_sha256",
    }


def test_committed_added_lines_pass_check() -> None:
    # Passes before the first Friday append too, when there is no added file yet.
    assert fa.verify(fl.DEFAULT_LEDGER_DIR) == []


def test_the_committed_ledger_and_its_schema_are_untouched() -> None:
    lines = [line for line in COMMITTED.read_bytes().replace(b"\r\n", b"\n").split(b"\n") if line]
    assert len(lines) >= 24
    assert hashlib.sha256(b"\n".join(lines[:24]) + b"\n").hexdigest() == COMMITTED_24
    assert fl.SCHEMA == "forward_ledger.v1"
    assert len(fl.ROW_KEYS) == 24 and "exp_overlay2" not in fl.ROW_KEYS
    rows = fl.read_ledger(COMMITTED.parent)
    assert fa.start_target(rows) == 1.0  # 11-03's F0001 exposure 0.5, doubled


def test_the_hooks_sit_at_the_end_of_three_commands_and_import_lazily() -> None:
    tree = ast.parse(LEDGER_MODULE.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ImportFrom | ast.Import):
            assert "forward_added" not in ast.dump(node)
    calls: dict[str, list[str]] = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            for inner in ast.walk(node):
                if (
                    isinstance(inner, ast.Attribute)
                    and isinstance(inner.value, ast.Name)
                    and inner.value.id == "forward_added"
                ):
                    calls.setdefault(node.name, []).append(inner.attr)
    assert calls == {
        "append_command": ["after_append"],
        "weekly_command": ["after_weekly"],
        "check_command": ["after_check"],
    }


def test_the_module_never_writes_or_patches_the_main_ledger() -> None:
    source = MODULE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign | ast.AugAssign):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                assert not (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "fl"
                )
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name) and node.func.value.id == "fl":
                assert node.func.attr not in {"append", "build_rows", "write_weekly"}
    for words in ("requests", "urllib", "httpx", "us_market", "SPY", "fmp_"):
        assert words not in source, words


# The calendar and the targets --------------------------------------------------------------------


def test_reset_rows_are_two_trading_days_after_the_last_day_of_a_month() -> None:
    for day, expected in (
        (date(2026, 10, 30), False),
        (date(2026, 11, 2), False),
        (date(2026, 11, 3), True),
        (date(2026, 11, 4), False),
        (date(2026, 12, 1), False),
        (date(2026, 12, 2), True),
        (date(2027, 1, 4), False),
        (date(2027, 1, 5), True),
    ):
        assert fa.is_reset(day) is expected, day
    assert fa.previous_day(date(2026, 11, 27)) == date(2026, 11, 25)
    assert fa.previous_day(date(2027, 1, 4)) == date(2026, 12, 31)


def test_the_reset_target_is_twice_the_ledgers_exposure() -> None:
    for value, expected in ((0.0, 0.0), (0.5, 1.0), (1.0, 2.0)):
        assert fa.ledger_target({"date": "2026-11-03", "exp_overlay": value}) == expected
    for bad in (0.7, None, True, "0.5"):
        with pytest.raises(fa.AddedLinesError):
            fa.ledger_target({"date": "2026-11-03", "exp_overlay": bad})


def test_the_first_target_is_the_latest_reset_on_or_before_the_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {"date": "2026-10-30", "exp_overlay": 1.0},
        {"date": "2026-11-02", "exp_overlay": 1.0},
        {"date": "2026-11-03", "exp_overlay": 0.5},
        {"date": "2026-11-19", "exp_overlay": 0.53},
        {"date": "2026-12-01", "exp_overlay": 0.55},
        {"date": "2026-12-02", "exp_overlay": 1.0},
    ]
    for start, expected in (
        (date(2026, 11, 19), 1.0),
        (date(2026, 12, 1), 1.0),
        (date(2026, 12, 2), 2.0),
    ):
        monkeypatch.setattr(fa, "ADDED_START", start)
        assert fa.start_target(rows) == expected
    monkeypatch.setattr(fa, "ADDED_START", date(2026, 11, 2))
    with pytest.raises(fa.AddedLinesError):
        fa.start_target(rows)


# One row ------------------------------------------------------------------------------------------


def test_the_day_return_borrows_above_one_at_sgov_plus_one_percent() -> None:
    q, s = 0.01, 0.0002
    borrow, value, notes = fa.overlay2(2.0, q, s)
    assert borrow == 1.0 and value == pytest.approx(2 * q - (s + 0.01 / 252)) and notes == []
    assert fa.overlay2(1.5, q, s)[1] == pytest.approx(1.5 * q - 0.5 * (s + 0.01 / 252))
    assert fa.overlay2(1.0, q, None) == (0.0, q, [])
    assert fa.overlay2(0.5, q, None) == (0.0, 0.5 * q, [])
    assert fa.overlay2(0.0, q, s) == (0.0, 0.0, [])
    borrow, value, notes = fa.overlay2(2.0, q, None)
    assert (borrow, value) == (1.0, None) and notes
    borrow, value, notes = fa.overlay2(1.0, None, s)
    assert (borrow, value) == (0.0, None) and notes
    assert fa.overlay2(None, q, s)[:2] == (None, None)


def test_a_fall_through_zero_wipes_the_line_out() -> None:
    assert fa.overlay2(2.0, -0.5, 0.0002) == (1.0, -1.0, [fa.WIPED])
    borrow, value, notes = fa.overlay2(2.0, -0.49, 0.0002)
    assert value == pytest.approx(-0.98 - (0.0002 + 0.01 / 252)) and notes == []


def test_the_exposure_drifts_and_zero_and_one_stay_put() -> None:
    row = {"exp_overlay2": 1.6, "qqq_tr": 0.02, "ret_overlay2": 0.03}
    assert fa.drift(row) == pytest.approx(1.6 * 1.02 / 1.03)
    for w in (0.0, 1.0):
        assert fa.drift({"exp_overlay2": w, "qqq_tr": None, "ret_overlay2": None}) == w
    assert fa.drift({"exp_overlay2": 2.0, "qqq_tr": None, "ret_overlay2": None}) is None
    assert fa.drift({"exp_overlay2": 2.0, "qqq_tr": 0.01, "ret_overlay2": None}) is None
    assert fa.drift({"exp_overlay2": None, "qqq_tr": 0.01, "ret_overlay2": 0.0}) is None


# Appending ----------------------------------------------------------------------------------------


def test_rows_start_at_the_start_and_hang_on_the_main_rows(
    daily_root: Path, ledger_dir: Path, started: None
) -> None:
    main = _main_through(ledger_dir, daily_root, END)
    rows = fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    assert rows == _rows(ledger_dir)
    raw = fl._ledger_lines(ledger_dir)
    after = [(m, line) for m, line in zip(main, raw, strict=True) if m["date"] >= "2026-11-05"]
    assert len(rows) == len(after)
    previous = None
    for k, (row, (m, line)) in enumerate(zip(rows, after, strict=True), 1):
        assert row.keys() == fa.ROW_KEYS
        assert row["schema_version"] == fa.SCHEMA and row["seq"] == k
        assert (row["date"], row["ledger_seq"]) == (m["date"], m["seq"])
        assert row["ledger_sha256"] == chain_digest(line)
        assert (row["qqq_tr"], row["price_file"]) == (m["qqq_tr"], m["price_file"])
        assert row["prev_sha256"] == (None if previous is None else chain_digest(previous))
        assert (row["code_commit"], row["code_modified"]) == (COMMIT, False)
        previous = fa._row_bytes(row)
    assert rows[0]["date"] == "2026-11-05" and rows[-1]["date"] == END.isoformat()
    assert fa.verify(ledger_dir) == []


def test_f0001_times_two_follows_the_written_rule(
    daily_root: Path, ledger_dir: Path, started: None
) -> None:
    main = _main_through(ledger_dir, daily_root, END)
    rows = fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    expected = _oracle(main)
    for row, (w, b, r) in zip(rows, expected, strict=True):
        assert row["exp_overlay2"] == pytest.approx(w, abs=1e-12), row["date"]
        assert row["borrow_overlay2"] == pytest.approx(b, abs=1e-12), row["date"]
        assert row["ret_overlay2"] == pytest.approx(r, abs=1e-12), row["date"]
        assert row["sgov_tr"] == pytest.approx(_sgov_return(date.fromisoformat(row["date"])))
    by_day = {row["date"]: row for row in rows}
    assert by_day["2026-11-05"]["target_overlay2"] == 1.0
    assert by_day["2026-12-01"]["target_overlay2"] == 1.0
    assert by_day["2026-12-02"]["target_overlay2"] == by_day["2026-12-02"]["exp_overlay2"] == 2.0
    assert by_day["2026-12-11"]["target_overlay2"] == 2.0
    assert by_day["2026-12-03"]["borrow_overlay2"] > 0.9
    assert by_day["2026-11-20"]["borrow_overlay2"] == 0.0
    assert all(row["exp_overlay2"] == 1.0 for row in rows if row["date"] < "2026-12-02")


def test_a_month_at_two_falling_twenty_percent_still_checks(
    daily_root: Path, ledger_dir: Path, started: None
) -> None:
    falls: dict[date, float | None] = {
        day: -0.032 for day in fl.trading_days(date(2026, 12, 3), END)
    }
    _main_through(ledger_dir, daily_root, END, qqq=falls)
    rows = fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    assert (1 - 0.032) ** len(falls) < 0.8
    assert max(row["exp_overlay2"] for row in rows) > 2.5
    assert fa.verify(ledger_dir) == []


def test_a_wipe_out_records_minus_one_and_empties_every_later_row(
    daily_root: Path, ledger_dir: Path, started: None
) -> None:
    resets = {date(2026, 11, 3): 1.0, date(2026, 12, 2): 1.0}
    fall: dict[date, float | None] = {date(2026, 11, 12): -0.55}
    _main_through(ledger_dir, daily_root, date(2026, 11, 20), resets=resets, qqq=fall)
    rows = fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    # The second append reads the wipe-out from the written rows, across the 12-02 reset.
    _main_through(ledger_dir, daily_root, END, resets=resets, qqq=fall)
    rows += fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    by_day = {row["date"]: row for row in rows}
    assert by_day["2026-11-05"]["exp_overlay2"] == 2.0
    hit = by_day["2026-11-12"]
    assert hit["ret_overlay2"] == -1.0 and fa.WIPED in hit["notes"]
    later = [row for row in rows if row["date"] > "2026-11-12"]
    assert later and all(
        row["exp_overlay2"] is None
        and row["borrow_overlay2"] is None
        and row["ret_overlay2"] is None
        for row in later
    )
    assert by_day["2026-12-02"]["target_overlay2"] == 2.0
    assert by_day["2026-12-02"]["notes"] == [f"之前{fa.WIPED}"]
    assert fa.verify(ledger_dir) == []


def test_a_missing_return_empties_the_line_until_the_next_reset(
    daily_root: Path, ledger_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fa, "ADDED_START", date(2026, 12, 2))
    for day in fl.trading_days(date(2026, 12, 14), date(2027, 1, 6)):
        _write_capture(daily_root, day)
        _write_snapshot(daily_root, day)
    resets = {**RESETS, date(2027, 1, 5): 1.0}
    _main_through(
        ledger_dir, daily_root, date(2027, 1, 6), qqq={date(2026, 12, 8): None}, resets=resets
    )
    rows = {row["date"]: row for row in fa.append(ledger_dir, daily_root, provenance=PROVENANCE)}
    assert rows["2026-12-08"]["exp_overlay2"] is not None
    assert rows["2026-12-08"]["ret_overlay2"] is None and rows["2026-12-08"]["notes"]
    for key in ("2026-12-09", "2026-12-31", "2027-01-04"):
        assert rows[key]["exp_overlay2"] is None and rows[key]["ret_overlay2"] is None, key
    assert rows["2027-01-05"]["exp_overlay2"] == rows["2027-01-05"]["target_overlay2"] == 2.0
    assert rows["2027-01-06"]["ret_overlay2"] is not None
    assert fa.verify(ledger_dir) == []


def test_at_zero_or_one_a_missing_return_does_not_pass_on(
    daily_root: Path, ledger_dir: Path, started: None
) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 11, 20), qqq={date(2026, 11, 9): None})
    rows = {row["date"]: row for row in fa.append(ledger_dir, daily_root, provenance=PROVENANCE)}
    assert rows["2026-11-09"]["exp_overlay2"] == 1.0 and rows["2026-11-09"]["ret_overlay2"] is None
    assert rows["2026-11-10"]["exp_overlay2"] == 1.0
    assert rows["2026-11-10"]["ret_overlay2"] == pytest.approx(_qqq_return(date(2026, 11, 10)))
    assert fa.verify(ledger_dir) == []


def test_borrowing_without_sgov_is_empty_and_not_borrowing_needs_no_sgov(
    tmp_path: Path, ledger_dir: Path, started: None
) -> None:
    root = tmp_path / "daily"
    gaps = (date(2026, 11, 9), date(2026, 12, 4))
    for day in fl.trading_days(fl.HELD_FROM, END):
        _write_capture(root, day, skip=gaps)
        _write_snapshot(root, day)
    _main_through(ledger_dir, root, END)
    rows = {row["date"]: row for row in fa.append(ledger_dir, root, provenance=PROVENANCE)}
    calm = rows["2026-11-09"]
    assert calm["sgov_tr"] is None and calm["borrow_overlay2"] == 0.0
    assert calm["ret_overlay2"] == pytest.approx(_qqq_return(date(2026, 11, 9)))
    lent = rows["2026-12-04"]
    assert lent["sgov_tr"] is None and lent["borrow_overlay2"] > 0.0
    assert lent["ret_overlay2"] is None and lent["notes"]
    assert rows["2026-12-07"]["exp_overlay2"] is None
    assert rows["2026-12-10"]["sgov_tr"] is not None and rows["2026-12-10"]["ret_overlay2"] is None
    assert fa.verify(ledger_dir) == []


def test_later_rows_continue_from_the_written_rows_without_recomputing(
    daily_root: Path, ledger_dir: Path, started: None
) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 12, 4))
    fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    written = _added_bytes(ledger_dir)
    last = _rows(ledger_dir)[-1]
    changed = SGOV.where(SGOV.index != pd.Timestamp("2026-12-03"), SGOV * 0.97)
    for day in fl.trading_days(date(2026, 12, 3), END):
        _write_capture(daily_root, day, sgov=changed)
    _main_through(ledger_dir, daily_root, END)
    rows = fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    assert _added_bytes(ledger_dir).startswith(written)
    assert rows[0]["date"] == "2026-12-07"
    assert rows[0]["exp_overlay2"] == pytest.approx(fa.drift(last))
    assert rows[0]["prev_sha256"] == chain_digest(written.splitlines()[-1])
    assert fa.append(ledger_dir, daily_root, provenance=PROVENANCE) == []
    assert fa.verify(ledger_dir) == []


def test_crlf_rows_are_kept_and_chained_as_lf(
    daily_root: Path, ledger_dir: Path, started: None
) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 11, 20))
    fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    path = ledger_dir / fa.ADDED_FILE
    path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n").rstrip(b"\n").rstrip(b"\r"))
    before = path.read_bytes()
    _main_through(ledger_dir, daily_root, date(2026, 11, 27))
    fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    assert path.read_bytes().startswith(before + b"\n")
    assert fa.verify(ledger_dir) == []


def test_nothing_is_written_before_the_start(daily_root: Path, ledger_dir: Path) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 11, 18))
    assert fa.append(ledger_dir, daily_root, provenance=PROVENANCE) == []
    assert not (ledger_dir / fa.ADDED_FILE).exists()
    assert fa.verify(ledger_dir) == []


@pytest.mark.parametrize(
    "provenance",
    [{**PROVENANCE, "code_modified": True}, {**PROVENANCE, "commit": "abc"}, {}],
)
def test_appending_needs_a_clean_commit(
    daily_root: Path, ledger_dir: Path, started: None, provenance: dict[str, Any]
) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 11, 20))
    with pytest.raises(fa.AddedLinesError):
        fa.append(ledger_dir, daily_root, provenance=provenance)
    assert not (ledger_dir / fa.ADDED_FILE).exists()


# The total-asset column ---------------------------------------------------------------------------


def test_the_total_column_holds_the_snapshot_from_two_trading_days_back(
    daily_root: Path, ledger_dir: Path, started: None
) -> None:
    main = _main_through(ledger_dir, daily_root, END)
    rows = {row["date"]: row for row in fa.append(ledger_dir, daily_root, provenance=PROVENANCE)}
    row = rows["2026-11-20"]  # held: 11-18 has no total, so the column is empty
    assert row["total_snapshot_file"] == fl.SNAPSHOT_REL.format(day="2026-11-18")
    assert row["total_min"] is None and row["exp_total_held"] is None and row["notes"]
    row = rows["2026-11-19"]  # held: 11-17's total band is not a band
    assert row["total_snapshot_file"] == fl.SNAPSHOT_REL.format(day="2026-11-17")
    assert row["total_max"] is None and row["ret_total"] is None and row["notes"]
    row = rows["2026-11-12"]  # held: 11-10 has no snapshot, so 11-09's carries on
    assert row["total_snapshot_file"] == fl.SNAPSHOT_REL.format(day="2026-11-09")
    low, high = _total(date(2026, 11, 9))
    assert (row["total_min"], row["total_max"]) == (low, high)
    assert row["exp_total_held"] == pytest.approx((low + high) / 2)
    assert row["ret_total"] == pytest.approx((low + high) / 2 * row["qqq_tr"])
    row = rows["2026-12-11"]
    low, high = _total(date(2026, 12, 9))
    assert row["exp_total_held"] == pytest.approx((low + high) / 2)
    by_date = {m["date"]: m for m in main}
    for day in ("2026-11-19", "2026-11-20"):
        assert rows[day]["exp_overlay2"] is not None and rows[day]["ret_overlay2"] is not None
        assert by_date[day]["exp_daily_held"] is not None
    assert fa.verify(ledger_dir) == []


def test_reading_a_total_band_never_raises(tmp_path: Path) -> None:
    root = tmp_path / "daily"
    day = date(2026, 11, 20)
    relative = fl.SNAPSHOT_REL.format(day=day.isoformat())
    assert fa.read_total(root, relative)[0] is None
    _write_snapshot(root, day)
    total, reason = fa.read_total(root, relative)
    assert total == fa.Total(*_total(day), relative) and reason is None
    for band in ((0.6, 0.5), (-0.1, 0.5), (0.2, 1.2)):
        _write_snapshot(root, day, total=band)
        assert fa.read_total(root, relative)[0] is None
    (root / relative).write_text("{not json", encoding="utf-8")
    total, reason = fa.read_total(root, relative)
    assert total is None and reason


# The command line: the main ledger first ---------------------------------------------------------


def _fake_main(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fl, "git_provenance", lambda anchor: dict(PROVENANCE))

    def main_append(
        ledger_dir: Path, daily_root: Path, through: date, *, provenance: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return _main_through(ledger_dir, daily_root, through)

    monkeypatch.setattr(fl, "append", main_append)


def _append_cli(ledger_dir: Path, daily_root: Path, through: str) -> Any:
    return CliRunner().invoke(
        fl.app,
        [
            "append",
            "--through",
            through,
            "--daily-root",
            str(daily_root),
            "--ledger-dir",
            str(ledger_dir),
        ],
    )


def test_append_writes_the_main_ledger_then_the_added_lines(
    daily_root: Path, ledger_dir: Path, started: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_main(monkeypatch)
    result = _append_cli(ledger_dir, daily_root, "2026-11-20")
    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    main_at = max(i for i, line in enumerate(lines) if line.startswith("forward_ledger: 追加"))
    added_at = min(i for i, line in enumerate(lines) if line.startswith("forward_added: "))
    assert main_at < added_at
    assert "forward_added: 追加 12 行，共 12 行" in result.output
    assert "forward_added: 2026-11-05 F0001×2 1.00" in result.output


def test_a_failure_in_the_added_lines_keeps_the_main_ledger_and_catches_up(
    daily_root: Path, ledger_dir: Path, started: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_main(monkeypatch)
    real = fa.read_sgov

    def broken(root: Path, price_file: str, day: date) -> float | None:
        if day == date(2026, 11, 13):
            raise fa.AddedLinesError("synthetic failure halfway")
        return real(root, price_file, day)

    monkeypatch.setattr(fa, "read_sgov", broken)
    result = _append_cli(ledger_dir, daily_root, "2026-11-20")
    assert result.exit_code == 1
    assert "forward_added: 加的线没有写（主账本照写）：synthetic failure halfway" in result.output
    assert len(fl.read_ledger(ledger_dir)) == len(
        fl.trading_days(fl.BACKFILL_START, date(2026, 11, 20))
    )
    assert not (ledger_dir / fa.ADDED_FILE).exists()
    monkeypatch.setattr(fa, "read_sgov", real)
    result = _append_cli(ledger_dir, daily_root, "2026-11-20")
    assert result.exit_code == 0, result.output
    assert "forward_ledger: 没有新的交易日" in result.output
    assert "forward_added: 追加 12 行，共 12 行" in result.output
    assert fa.verify(ledger_dir) == []


def test_broken_added_rows_block_only_the_added_file(
    daily_root: Path, ledger_dir: Path, started: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_main(monkeypatch)
    assert _append_cli(ledger_dir, daily_root, "2026-11-20").exit_code == 0
    path = ledger_dir / fa.ADDED_FILE
    lines = path.read_bytes().splitlines()
    row = json.loads(lines[2])
    row["ret_overlay2"] += 0.001
    path.write_bytes(b"\n".join([*lines[:2], fa._row_bytes(row), *lines[3:]]) + b"\n")
    broken = path.read_bytes()
    result = _append_cli(ledger_dir, daily_root, "2026-11-27")
    assert result.exit_code == 1
    assert "forward_added: 加的线没有写（主账本照写）" in result.output
    assert "line 3" in result.output
    assert fl.read_ledger(ledger_dir)[-1]["date"] == "2026-11-27"
    assert path.read_bytes() == broken


# Checking -----------------------------------------------------------------------------------------


@pytest.fixture()
def written(daily_root: Path, ledger_dir: Path, started: None) -> Path:
    _main_through(ledger_dir, daily_root, END)
    fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    return ledger_dir


def _edit(ledger_dir: Path, line: int, change: Callable[[dict[str, Any]], None]) -> None:
    path = ledger_dir / fa.ADDED_FILE
    lines = path.read_bytes().splitlines()
    row = json.loads(lines[line - 1])
    change(row)
    lines[line - 1] = fa._row_bytes(row)
    path.write_bytes(b"\n".join(lines) + b"\n")


def _set(key: str, value: Any) -> Callable[[dict[str, Any]], None]:
    def change(row: dict[str, Any]) -> None:
        row[key] = value

    return change


def _shift(key: str, by: float) -> Callable[[dict[str, Any]], None]:
    def change(row: dict[str, Any]) -> None:
        row[key] = row[key] + by

    return change


# Line numbers in the synthetic file: 1 = 11-05, 11 = 11-19, 12 = 11-20, 19 = 12-02, 20 = 12-03.
@pytest.mark.parametrize(
    ("line", "change", "words"),
    [
        (5, _set("target_overlay2", 3.0), "target_overlay2 is not 0, 1 or 2"),
        (5, _set("target_overlay2", 2.0), "target_overlay2 changed off a reset row"),
        (19, _set("target_overlay2", 1.0), "twice the ledger's exp_overlay on a reset row"),
        (1, _set("target_overlay2", 2.0), "twice the target before ADDED_START"),
        (20, _shift("exp_overlay2", 1e-9), "reset and drift rule"),
        (5, _set("exp_overlay2", -0.5), "exp_overlay2 is negative"),
        (5, _set("exp_overlay2", float("nan")), "not a finite number"),
        (20, _shift("borrow_overlay2", 0.01), "borrow_overlay2"),
        (20, _shift("ret_overlay2", 1e-9), "ret_overlay2 does not equal"),
        (5, _shift("ret_overlay2", 1e-9), "ret_overlay2 does not equal"),
        (5, _shift("exp_total_held", 0.01), "exp_total_held is not the band midpoint"),
        (5, _set("exp_total_held", "0.4"), "exp_total_held is not the band midpoint"),
        (5, _shift("ret_total", 1e-6), "ret_total does not equal"),
        (5, _set("total_min", 0.9), "total_min/total_max is not a valid band"),
        (11, _set("exp_total_held", 0.4), "must be None without a band"),
        (5, _set("ledger_sha256", "0" * 64), "ledger_sha256 does not match"),
        (5, _shift("qqq_tr", 1e-6), "qqq_tr or price_file differs"),
        (5, _set("price_file", fl.PRICES_REL.format(day="2026-11-12")), "qqq_tr or price_file"),
        (5, _set("date", "2026-11-12"), "date does not match"),
        (5, _shift("ledger_seq", 1), "ledger_seq does not continue"),
        (5, _set("seq", 6), "seq is not 5"),
        (5, _set("schema_version", "forward_added.v0"), "schema_version does not match"),
        (5, _set("extra", 1), "keys do not match ROW_KEYS"),
        (5, _set("prev_sha256", "0" * 64), "prev_sha256 does not match"),
    ],
)
def test_check_catches_each_broken_identity(
    written: Path, line: int, change: Callable[[dict[str, Any]], None], words: str
) -> None:
    assert fa.verify(written) == []
    _edit(written, line, change)
    problems = fa.verify(written)
    assert problems and words in problems[0] and f"line {line}:" in problems[0], problems


def test_check_catches_a_missing_row_and_a_bad_line(written: Path) -> None:
    path = written / fa.ADDED_FILE
    lines = path.read_bytes().splitlines()
    path.write_bytes(b"\n".join([*lines[:3], *lines[4:]]) + b"\n")
    assert "line 4:" in fa.verify(written)[0]
    path.write_bytes(b"\n".join([*lines[:3], b"{not json", *lines[4:]]) + b"\n")
    assert "line 4: not JSON" in fa.verify(written)[0]
    path.write_bytes(b"\n".join(lines[1:]) + b"\n")
    assert "line 1:" in fa.verify(written)[0]


def test_check_wants_every_row_after_a_wipe_out_empty(
    daily_root: Path, ledger_dir: Path, started: None
) -> None:
    resets = {date(2026, 11, 3): 1.0, date(2026, 12, 2): 1.0}
    _main_through(ledger_dir, daily_root, END, resets=resets, qqq={date(2026, 11, 12): -0.55})
    fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    assert fa.verify(ledger_dir) == []
    _edit(ledger_dir, 19, _set("exp_overlay2", 2.0))
    assert "rows after a wipe-out must be empty" in fa.verify(ledger_dir)[0]


def test_the_weekly_section_after_a_wipe_out(
    daily_root: Path, ledger_dir: Path, started: None
) -> None:
    resets = {date(2026, 11, 3): 1.0, date(2026, 12, 2): 1.0}
    _main_through(ledger_dir, daily_root, END, resets=resets, qqq={date(2026, 11, 12): -0.55})
    fa.append(ledger_dir, daily_root, provenance=PROVENANCE)
    text = "\n".join(fa.render_section(fl.read_ledger(ledger_dir), fa.read_added(ledger_dir)))
    assert "2026-11-12 净值归零，对买入持有和对 2 × 同期 F0001 的累计对数差不印。" in text
    assert "| F0001 × 2 | 26 | 20 | -100.00% | -100.00% |" in text
    fl.write_weekly(ledger_dir, daily_root)
    assert fa.after_weekly(ledger_dir, lambda line: None) is True
    assert fa.SECTION in (ledger_dir / fl.WEEKLY_FILE).read_text(encoding="utf-8")


def test_the_added_file_may_lag_the_main_ledger(written: Path, daily_root: Path) -> None:
    _main_through(written, daily_root, date(2026, 12, 18))
    assert fa.verify(written) == []
    text = "\n".join(fa.render_section(fl.read_ledger(written), fa.read_added(written)))
    assert "加的线落后主账本 5 行。" in text


def test_check_day_rereads_the_capture_and_the_snapshot(written: Path, daily_root: Path) -> None:
    day = date(2026, 12, 3)
    assert fa.check_day(written, daily_root, day) == []
    assert fa.check_day(written, daily_root, date(2026, 11, 4)) == []  # before the added rows
    changed = SGOV.where(SGOV.index != pd.Timestamp(day), SGOV * 1.001)
    _write_capture(daily_root, day, sgov=changed)
    assert any("sgov_tr does not match" in p for p in fa.check_day(written, daily_root, day))
    _write_capture(daily_root, day)
    held = date(2026, 12, 1)
    _write_snapshot(daily_root, held, total=(0.1, 0.2))
    assert any("total band does not match" in p for p in fa.check_day(written, daily_root, day))
    _write_snapshot(daily_root, held, final=(0.0, 0.1))
    problems = fa.check_day(written, daily_root, day)
    assert any("final band midpoint is not exp_daily_held" in p for p in problems)
    (daily_root / fl.SNAPSHOT_REL.format(day=held.isoformat())).unlink()
    problems = fa.check_day(written, daily_root, day)
    assert any("total_snapshot_file is not the held snapshot" in p for p in problems)


def test_check_command_reports_the_added_lines_after_the_main_ledger(
    written: Path, daily_root: Path
) -> None:
    runner = CliRunner()
    result = runner.invoke(
        fl.app,
        [
            "check",
            "--ledger-dir",
            str(written),
            "--day",
            "2026-12-03",
            "--daily-root",
            str(daily_root),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "forward_ledger: 账本完好" in result.output
    assert "forward_added: 加的线完好，26 行（2026-11-05..2026-12-11）" in result.output
    _write_snapshot(daily_root, date(2026, 12, 1), total=(0.1, 0.2))  # held on 12-03
    day_check = ["check", "--ledger-dir", str(written), "--daily-root", str(daily_root)]
    result = runner.invoke(fl.app, [*day_check, "--day", "2026-12-03"])
    assert result.exit_code == 1
    assert "forward_ledger: 账本完好" in result.output
    assert "total band does not match" in result.output
    _edit(written, 7, _shift("ret_overlay2", 0.01))
    result = runner.invoke(fl.app, ["check", "--ledger-dir", str(written)])
    assert result.exit_code == 1
    assert "forward_ledger: 账本完好" in result.output
    assert "forward_added: added_lines.jsonl line 7: ret_overlay2" in result.output


# The weekly section -------------------------------------------------------------------------------


def test_weekly_keeps_every_original_section_and_adds_the_new_one(
    written: Path, daily_root: Path
) -> None:
    fl.write_weekly(written, daily_root)
    original = (written / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    footer = "\nproduction_effect=none；broker_action=none。\n"
    fa.extend_weekly(written)
    text = (written / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    assert text.startswith(original[: -len(footer)])
    assert text.endswith(footer)
    section = text[len(original) - len(footer) :]
    assert f"{fa.SECTION}（自 {fa.ADDED_START}" in section
    for label in (
        "F0001 × 2",
        "买入持有",
        "F0001（主账本）",
        "日报总资产口径",
        "日报模型（主账本）",
    ):
        assert f"| {label} | 26 |" in section, label
    assert "加的线落后主账本 0 行。" in section
    with pytest.raises(fa.AddedLinesError):
        fa.extend_weekly(written)


def test_the_weekly_section_prints_the_borrowing_numbers(written: Path) -> None:
    main = fl.read_ledger(written)
    added = fa.read_added(written)
    text = "\n".join(fa.render_section(main, added))
    by_seq = {m["seq"]: m for m in main}
    weights = [row["exp_overlay2"] for row in added]
    borrows = [row["borrow_overlay2"] for row in added]
    interest = sum(
        b * (row["sgov_tr"] + 0.01 / 252) for b, row in zip(borrows, added, strict=True) if b
    )
    hold = sum(math.log1p(by_seq[row["ledger_seq"]]["qqq_tr"]) for row in added)
    mine = sum(math.log1p(row["ret_overlay2"]) for row in added)
    double = sum(math.log1p(2 * by_seq[row["ledger_seq"]]["ret_overlay"]) for row in added)
    sgov = [row["sgov_tr"] for row in added]
    annual = math.exp(sum(math.log1p(s) for s in sgov) * 252 / len(sgov)) - 1
    assert f"平均借的比例 {sum(borrows) / len(borrows):.2f}" in text
    assert f"期间最大的 w {max(weights):.2f}" in text
    assert f"累计付的利息 {interest * 100:.2f} 点" in text
    assert f"对买入持有的累计对数差 {(mine - hold) * 100:+.2f} 点" in text
    assert f"对 2 × 同期 F0001 {(mine - double) * 100:+.2f} 点（{len(added)} 个交易日）" in text
    assert f"期间 SGOV 年化 {annual * 100:+.2f}%" in text
    assert f"小于 0 或大于 0.1% 的：{ODD.isoformat()}、{HIGH.isoformat()}。" in text
    wealth = np.cumprod([1.0] + [1.0 + row["ret_overlay2"] for row in added])
    drawdown = float(np.min(wealth / np.maximum.accumulate(wealth) - 1.0))
    line = (
        f"| F0001 × 2 | {len(added)} | 0 | {(wealth[-1] - 1.0) * 100:+.2f}% | "
        f"{drawdown * 100:+.2f}% | {sum(weights) / len(weights):.2f} |"
    )
    assert line in text
    idle = [row["ret_overlay2"] + max(1 - row["exp_overlay2"], 0) * row["sgov_tr"] for row in added]
    assert f"F0001 × 2 累计 {(float(np.prod([1 + v for v in idle])) - 1) * 100:+.2f}%" in text


def test_the_weekly_section_before_any_row(daily_root: Path, ledger_dir: Path) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 11, 18))
    text = "\n".join(fa.render_section(fl.read_ledger(ledger_dir), []))
    assert "还没有行。" in text and "加的线落后主账本 0 行。" in text


def test_weekly_command_adds_the_section(written: Path, daily_root: Path) -> None:
    result = CliRunner().invoke(
        fl.app, ["weekly", "--ledger-dir", str(written), "--daily-root", str(daily_root)]
    )
    assert result.exit_code == 0, result.output
    assert "forward_added: 周汇总加了「加的线」一节" in result.output
    assert fa.SECTION in (written / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    _edit(written, 3, _shift("ret_overlay2", 0.01))
    result = CliRunner().invoke(fl.app, ["weekly", "--ledger-dir", str(written)])
    assert result.exit_code == 1
    assert "forward_ledger: 周汇总已写" in result.output
    assert "forward_added: 周汇总没有加「加的线」一节" in result.output


# Records ------------------------------------------------------------------------------------------


def test_the_records_name_this_run() -> None:
    assert "TRADING-2565-135" in REQUIREMENTS.read_text(encoding="utf-8")
    assert "research_probes/forward_added.py" in SYSTEM_FLOW.read_text(encoding="utf-8")
    readme = README.read_text(encoding="utf-8")
    for words in ("## 加的线", fa.ADDED_FILE, "2026-11-19", "净值归零", "total_asset_ai_band"):
        assert words in readme, words
    records = RECORDS.read_text(encoding="utf-8")
    assert "F0009" in records and "2026-11-19" in records
    assert "F0001 × 2" in records
