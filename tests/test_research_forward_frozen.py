"""Acceptance tests for TRADING-2565-137: the forward ledger's frozen line.

Requirement: knowledge qqq-options/auto-loop/period7/
forward-ledger-frozen-line-requirements-2026-11-16.md (the same-drawdown table's section 5.5
and Baker's 2026-11-16 freeze choice; reviewed by 「研究复盘 11-14」). The picked
x2_s5_under_ladder line hangs on the main ledger's rows, walks QQQ closes from each row's own
price file from the anchor, trades one close after each decision, and never blocks the main
ledger or the added lines. Synthetic files only.
"""

from __future__ import annotations

import ast
import hashlib
import json
import math
import shutil
from collections.abc import Callable, Iterator
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.research_probes import entry_structure_shift as es
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import forward_added as fa
from ai_trading_system.research_probes import forward_frozen as ff
from ai_trading_system.research_probes import forward_ledger as fl
from ai_trading_system.research_probes import structure_shift_ladder as sl
from ai_trading_system.research_probes import structure_shift_periods as sp

MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "forward_frozen.py"
LEDGER_MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "forward_ledger.py"
REQUIREMENTS = PROJECT_ROOT / "docs" / "requirements" / "TRADING-2565_Research_Bench.md"
SYSTEM_FLOW = PROJECT_ROOT / "docs" / "system_flow.md"
README = PROJECT_ROOT / "research" / "_forward" / "README.md"
RECORDS = PROJECT_ROOT / "research" / "_loop" / "forward_records.md"
CHARTER = PROJECT_ROOT / "research" / "_loop" / "charter_p7.md"
COMMITTED = PROJECT_ROOT / "research" / "_forward" / fl.LEDGER_FILE
# The first 24 committed rows (2026-10-12..2026-11-12) as main 44dbd582 holds them, LF-joined.
COMMITTED_24 = "f20e12cd6dfaa44271a1a1118a68f3a3ef791ae3ea90db8c793847a23264d640"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
PROVENANCE = {"commit": COMMIT, "branch": "main", "code_modified": False}

# Synthetic QQQ: drift segments plus a ten-trading-day wiggle, on the real trading calendar.
DAYS = fl.trading_days(date(2024, 1, 2), date(2027, 6, 30))
PRE = (
    (date(2025, 2, 18), 0.0010),
    (date(2025, 3, 24), -0.0090),  # the anchor's trip: X2, the 0.9 tranche, back at X
    (date(2025, 4, 7), -0.0020),
    (date(2025, 5, 30), 0.0070),
    (date(2026, 2, 20), 0.0009),
    (date(2026, 3, 27), -0.0060),  # the "printed" trip: X2, then a shift purchase with a floor
    (date(2026, 4, 3), -0.0005),
    (date(2026, 5, 15), 0.0060),
    (date(2026, 12, 1), 0.0008),
)
POSTS: dict[str, tuple[tuple[date, float], ...]] = {
    # X2 2026-12-16, sold 12-17, 0.9 tranche 2027-01-07, back at X 2027-02-25.
    "recovery": (
        (date(2027, 1, 8), -0.0090),
        (date(2027, 1, 22), -0.0020),
        (date(2027, 3, 5), 0.0070),
        (date(2027, 6, 30), 0.0008),
    ),
    # X2 2026-12-14, four tranches 12-30, 2027-01-15, 02-03, 02-23 (the last one buys the rest).
    "ladder": (
        (date(2027, 2, 26), -0.0110),
        (date(2027, 3, 12), -0.0010),
        (date(2027, 6, 30), 0.0040),
    ),
    # X2 2027-01-12, shift 02-04 with a floor, L 02-11, sold 02-12, shift 03-01.
    "stop": (
        (date(2026, 12, 22), -0.003),
        (date(2027, 1, 13), -0.003),
        (date(2027, 1, 18), 0.006),
        (date(2027, 2, 12), 0.0),
        (date(2027, 2, 18), -0.015),
        (date(2027, 3, 1), 0.01),
        (date(2027, 6, 30), 0.001),
    ),
    # X2 2026-12-10, sold 12-11, a v0_close entry 12-15.
    "entry": (
        (date(2026, 12, 10), -0.015),
        (date(2026, 12, 26), 0.01),
        (date(2027, 1, 18), 0.015),
        (date(2027, 2, 5), 0.015),
        (date(2027, 2, 14), 0.015),
        (date(2027, 2, 23), 0.015),
        (date(2027, 6, 30), 0.001),
    ),
}
END = date(2027, 3, 31)
CAPTURE_FROM = date(2026, 11, 16)
# The synthetic stand-ins for the table's anchor and printed trip (same shapes as the real ones).
ANCHOR = date(2025, 5, 6)
ANCHOR_SALE = date(2025, 3, 5)
ANCHOR_FILL = date(2025, 3, 21)
PRINTED = (
    {
        "decision": "2026-03-11",
        "reason": "X2",
        "sell": "2026-03-12",
        "buy": "2026-05-14",
        "how": "shift",
        "fills": (),
        "floor_date": "2026-03-30",
    },
)
PRINTED_FLOOR = date(2026, 3, 30)


def _path(post: tuple[tuple[date, float], ...]) -> pd.Series:
    segments = (*PRE, *post)
    values = [100.0]
    for day in DAYS[1:]:
        drift = next((mu for end, mu in segments if day <= end), 0.0)
        values.append(values[-1] * (1.0 + drift + 0.004 * math.sin(day.toordinal() / 2.3)))
    # The capture files carry ten decimals; every oracle reads the same digits.
    return pd.Series(values, index=pd.DatetimeIndex(DAYS)).round(10)


PATHS = {name: _path(post) for name, post in POSTS.items()}
SGOV = pd.Series(
    [100.0 * (1.00016**k) * (1.0 + 0.00001 * (k % 3)) for k in range(len(DAYS))],
    index=pd.DatetimeIndex(DAYS),
).round(10)
ANCHOR_AT = float(
    PATHS["recovery"][pd.Timestamp(ANCHOR_FILL)] / PATHS["recovery"][pd.Timestamp(ANCHOR_SALE)]
)


# Writing synthetic files --------------------------------------------------------------------------


def _write_capture(
    root: Path,
    day: date,
    qqq: pd.Series,
    *,
    through: date | None = None,
    drop: tuple[date, ...] = (),
    sgov: pd.Series | None = SGOV,
) -> None:
    """One capture folder's prices_daily.csv: QQQ and SGOV from 2024-01-02 to ``through``."""
    limit = pd.Timestamp(through or day)
    rows = []
    for stamp in qqq.index[qqq.index <= limit]:
        text = stamp.date().isoformat()
        if stamp.date() not in drop:
            rows.append(f"{text},QQQ,1,1,1,1,{qqq[stamp]:.10f},100")
        if sgov is not None:
            rows.append(f"{text},SGOV,1,1,1,1,{sgov[stamp]:.10f},100")
    path = root / fl.PRICES_REL.format(day=day.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )


def _q(series: pd.Series, day: date) -> float:
    """The return on ``day``: its value over the previous trading day's, minus one."""
    position = int(series.index.searchsorted(pd.Timestamp(day)))
    values = series.to_numpy(dtype=float)
    return float(values[position] / values[position - 1] - 1.0)


def _bumped(series: pd.Series, day: str, factor: float) -> pd.Series:
    """A copy with one day's value multiplied by ``factor``."""
    return series.where(series.index != pd.Timestamp(day), series * factor)


def _main_through(
    ledger_dir: Path,
    qqq: pd.Series,
    through: date,
    *,
    missing: tuple[date, ...] = (),
) -> list[dict[str, Any]]:
    """Valid main-ledger rows up to ``through`` (the main ledger's identities, simplified)."""
    existing = fl.read_ledger(ledger_dir)
    raw = fl._ledger_lines(ledger_dir)
    prev = chain_digest(raw[-1]) if raw else None
    start = (
        fl.BACKFILL_START
        if not existing
        else fl._next_day(date.fromisoformat(existing[-1]["date"]))
    )
    rows: list[dict[str, Any]] = []
    for day in fl.trading_days(start, through):
        q = None if day in missing else _q(qqq, day)
        row = {
            "schema_version": fl.SCHEMA,
            "seq": len(existing) + len(rows) + 1,
            "date": day.isoformat(),
            "backfill": day < fl.FORWARD_START,
            "qqq_tr": q,
            "price_file": fl.PRICES_REL.format(day=day.isoformat()),
            "price_last_date": day.isoformat(),
            "exp_daily": None,
            "daily_min": None,
            "daily_max": None,
            "daily_label": None,
            "daily_model_mid": None,
            "snapshot_file": None,
            "exp_daily_held": None,
            "exp_fixed": fl.FIXED_EXPOSURE,
            "overlay": fl.OVERLAY_ID,
            "exp_overlay": 0.5,
            "ret_daily": None,
            "ret_fixed": fl._product(fl.FIXED_EXPOSURE, q),
            "ret_overlay": fl._product(0.5, q),
            "notes": [],
            "code_commit": COMMIT,
            "code_modified": False,
            "prev_sha256": prev,
        }
        rows.append(row)
        prev = chain_digest(fl._row_bytes(row))
    with (ledger_dir / fl.LEDGER_FILE).open("ab") as sink:
        sink.write(b"".join(fl._row_bytes(row) + b"\n" for row in rows))
    assert fl.verify(ledger_dir) == []
    return rows


def _build(root: Path, scenario: str) -> Path:
    daily = root / "daily"
    for day in fl.trading_days(CAPTURE_FROM, END):
        _write_capture(daily, day, PATHS[scenario])
    ledger = root / "forward"
    ledger.mkdir()
    _main_through(ledger, PATHS[scenario], END)
    return root


@pytest.fixture(scope="session")
def built(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    return {name: _build(tmp_path_factory.mktemp(name), name) for name in POSTS}


def _point_at_synthetic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ff, "ANCHOR", ANCHOR)
    monkeypatch.setattr(ff, "ANCHOR_SALE", ANCHOR_SALE)
    monkeypatch.setattr(ff, "ANCHOR_FILL", ANCHOR_FILL)
    monkeypatch.setattr(ff, "ANCHOR_AT", ANCHOR_AT)
    monkeypatch.setattr(ff, "PRINTED", PRINTED)
    monkeypatch.setattr(ff, "PRINTED_FLOOR", PRINTED_FLOOR)


@pytest.fixture()
def synthetic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the guard at the synthetic anchor and printed trip."""
    _point_at_synthetic(monkeypatch)


def _copy(built: dict[str, Path], scenario: str, tmp_path: Path) -> tuple[Path, Path]:
    target = tmp_path / scenario
    shutil.copytree(built[scenario], target)
    return target / "daily", target / "forward"


@pytest.fixture(scope="session")
def appended(
    built: dict[str, Path], tmp_path_factory: pytest.TempPathFactory
) -> Callable[[str], Path]:
    """A scenario with every frozen row appended once, for tests that only read or edit rows."""
    cache: dict[str, Path] = {}

    def get(scenario: str) -> Path:
        if scenario not in cache:
            root = tmp_path_factory.mktemp(f"appended-{scenario}")
            with pytest.MonkeyPatch.context() as monkeypatch:
                _point_at_synthetic(monkeypatch)
                daily, ledger = _copy(built, scenario, root)
                ff.append(ledger, daily, provenance=PROVENANCE)
            cache[scenario] = root / scenario
        return cache[scenario]

    return get


def _file(day: str) -> str:
    return fl.PRICES_REL.format(day=day)


@pytest.fixture()
def recovery(built: dict[str, Path], tmp_path: Path, synthetic: None) -> tuple[Path, Path]:
    return _copy(built, "recovery", tmp_path)


def _append(daily: Path, ledger: Path) -> list[dict[str, Any]]:
    return ff.append(ledger, daily, provenance=PROVENANCE)


def _rows(ledger: Path) -> list[dict[str, Any]]:
    path = ledger / ff.FROZEN_FILE
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_bytes().splitlines() if line]


def _truncate_main(ledger: Path, through: date) -> None:
    lines = fl._ledger_lines(ledger)
    keep = [line for line in lines if json.loads(line)["date"] <= through.isoformat()]
    (ledger / fl.LEDGER_FILE).write_bytes(b"".join(line + b"\n" for line in keep))


def _extend_main(ledger: Path, scenario: str, through: date) -> None:
    _main_through(ledger, PATHS[scenario], through)


# The oracle: the requirement's words, written independently of the module --------------------


def _decisions(
    qqq: pd.Series, through: date
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Rule decisions by close from the anchor, walked by the library over closes to ``through``."""
    kept = qqq[(qqq.index >= pd.Timestamp(ff.WALK_FROM)) & (qqq.index <= pd.Timestamp(through))]
    dates = [stamp.date() for stamp in kept.index]
    close = [float(v) for v in kept]
    line = ec.moving_average(close)
    moves = es.shifts(close, line, 5, ec.SMA_DAYS)
    path = sl.walk_ladder(
        xc.prepare(dates, close),
        sp.v0_entries(close, 0),
        moves,
        dates.index(ANCHOR),
        lambda rule, data, b: sp.exit_under(rule, data, line, b),
    )
    out: dict[str, list[dict[str, Any]]] = {}
    for trip in path.route.trips:
        out.setdefault(trip["decision"], []).append({"kind": "fire", "reason": trip["reason"]})
        for fill in trip["fills"]:
            out.setdefault(fill["date"], []).append(
                {"kind": "fill", "level": fill["level"], "share": fill["share"]}
            )
        if trip["how"] != "none":
            floor = None
            if trip["how"] == "shift":
                sold = dates.index(date.fromisoformat(trip["sell"]))
                move = next(m for m in moves if m.entry > sold)
                floor = dates[move.low].isoformat()
            out.setdefault(trip["buy"], []).append(
                {"kind": "full", "how": trip["how"], "floor_date": floor}
            )
    return out, list(path.route.trips)


def _share(qqq: pd.Series, trips: list[dict[str, Any]], day: date) -> float:
    """The rule's cost-free QQQ share after close ``day``."""
    for trip in trips:
        sold = date.fromisoformat(trip["sell"])
        bought = None if trip["buy"] is None else date.fromisoformat(trip["buy"])
        if sold <= day and (bought is None or day < bought):
            held = sum(
                f["share"] * qqq[pd.Timestamp(day)] / qqq[pd.Timestamp(f["date"])]
                for f in trip["fills"]
                if f["date"] <= day.isoformat()
            )
            idle = 1.0 - sum(f["share"] for f in trip["fills"] if f["date"] <= day.isoformat())
            return float(held / (held + idle))
    return 1.0


def _target(actions: list[dict[str, Any]], share: float) -> float | None:
    kinds = [a["kind"] for a in actions]
    if "fire" in kinds:
        return 0.0
    if "full" in kinds:
        return 1.0
    if "fill" in kinds:
        return share
    return None


def _drift(w: float | None, q: float | None) -> float | None:
    if w is None or w in (0.0, 1.0):
        return w
    return None if q is None else w * (1.0 + q) / (1.0 + w * q)


def _oracle(
    scenario: str, start: date, through: date, missing: tuple[date, ...] = ()
) -> list[tuple[float | None, float | None, float | None]]:
    """(trade, w, r) for each row from ``start``; the ledger trades one close after a decision."""
    qqq = PATHS[scenario]
    decisions, trips = _decisions(qqq, END)
    days = fl.trading_days(ANCHOR, through)

    def decided(j: int) -> float | None:
        """The target decided at close days[j]; the anchor's own close bought everything."""
        if j == 0:
            return 1.0
        return _target(decisions.get(days[j].isoformat(), []), _share(qqq, trips, days[j]))

    out = []
    w: float | None = None
    for k in range(1, len(days)):
        if k >= 2:
            traded = decided(k - 2)  # the trade on close days[k-1]
            before = days[k - 1]
            q_before = None if before in missing else _q(qqq, before)
            w = traded if traded is not None else _drift(w, q_before)
        if days[k] >= start:
            q = None if days[k] in missing else _q(qqq, days[k])
            out.append((decided(k - 1), w, None if w is None or q is None else w * q))
    return out


# Constants, records and the shape of the change ---------------------------------------------------


def test_constants_name_the_run_the_decision_and_the_table() -> None:
    assert ff.RUN_ID == "TRADING-2565-137"
    assert ff.OWNER_DECISION == "owner_decision:TRADING-2565:2026-11-10:same_drawdown_table"
    assert ff.SCHEMA == "forward_frozen.v1"
    assert ff.FROZEN_FILE == "frozen_lines.jsonl"
    assert ff.LINE == "x2_s5_under_ladder"
    assert ff.FROZEN_START == date(2026, 11, 23)
    assert ff.WALK_FROM == date(2024, 1, 2)
    assert ff.ANCHOR == date(2025, 5, 12)
    assert ff.SNAPSHOT_END == date(2026, 10, 12)
    assert ff.PRINTED == (
        {
            "decision": "2026-03-20",
            "reason": "X2",
            "sell": "2026-03-23",
            "buy": "2026-04-13",
            "how": "shift",
            "fills": (),
            "floor_date": "2026-03-30",
        },
    )
    assert ff.PRINTED_FLOOR == date(2026, 3, 30)
    assert (ff.ANCHOR_SALE, ff.ANCHOR_FILL) == (date(2025, 3, 7), date(2025, 4, 4))
    assert abs(ff.ANCHOR_AT - 0.8607) < 1e-4 and ff.ANCHOR_AT_TOLERANCE == 0.001
    assert ff.ROW_KEYS == {
        "schema_version", "seq", "date", "ledger_seq", "ledger_sha256", "qqq_tr",
        "price_file", "rule_price_file", "sgov_tr", "line", "rule_actions", "rule_frac",
        "trade_frozen", "exp_frozen", "ret_frozen", "phase", "sale_date", "filled_levels",
        "close_vs_x", "high_date", "close_vs_high", "floor_date", "close_vs_floor", "close_vs_line",
        "notes", "code_commit", "code_modified", "prev_sha256",
    }  # fmt: skip


def test_committed_frozen_lines_pass_check() -> None:
    # Passes before the first row too, when there is no frozen file yet.
    assert ff.verify(fl.DEFAULT_LEDGER_DIR) == []


def test_the_committed_ledger_and_the_added_lines_are_untouched() -> None:
    lines = [line for line in COMMITTED.read_bytes().replace(b"\r\n", b"\n").split(b"\n") if line]
    assert hashlib.sha256(b"\n".join(lines[:24]) + b"\n").hexdigest() == COMMITTED_24
    assert fl.SCHEMA == "forward_ledger.v1" and len(fl.ROW_KEYS) == 24
    assert fa.SCHEMA == "forward_added.v1" and len(fa.ROW_KEYS) == 21
    assert fa.ADDED_START == date(2026, 11, 19)


def _command_calls(module: str) -> dict[str, list[tuple[int, str]]]:
    tree = ast.parse(LEDGER_MODULE.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ImportFrom | ast.Import):
            assert module not in ast.dump(node)
    calls: dict[str, list[tuple[int, str]]] = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            for inner in ast.walk(node):
                if (
                    isinstance(inner, ast.Attribute)
                    and isinstance(inner.value, ast.Name)
                    and inner.value.id == module
                ):
                    calls.setdefault(node.name, []).append((inner.lineno, inner.attr))
    return calls


def test_the_hooks_follow_the_added_lines_at_the_end_of_three_commands() -> None:
    frozen = _command_calls("forward_frozen")
    added = _command_calls("forward_added")
    assert {name: [attr for _, attr in calls] for name, calls in frozen.items()} == {
        "append_command": ["after_append"],
        "weekly_command": ["after_weekly"],
        "check_command": ["after_check"],
    }
    for name in frozen:
        assert added[name][0][0] < frozen[name][0][0], name


def test_the_module_never_writes_the_ledger_or_the_added_lines() -> None:
    source = MODULE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign | ast.AugAssign):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                assert not (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id in {"fl", "fa", "sl", "sp", "es", "ec", "xc"}
                )
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name) and node.func.value.id in {"fl", "fa"}:
                assert node.func.attr not in {"append", "build_rows", "write_weekly"}
    for words in ("requests", "urllib", "httpx", "us_market", "SPY", "fmp_", "ADDED_FILE"):
        assert words not in source, words


def test_the_rule_is_reused_from_the_table_not_rewritten() -> None:
    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    used = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
    }
    assert {
        "walk_ladder",
        "exit_under",
        "v0_entries",
        "shifts",
        "prepare",
        "moving_average",
    } <= used
    defined = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
    assert not {"walk_ladder", "exit_under", "v0_entries", "shifts"} & defined


def test_the_records_name_this_run() -> None:
    readme = README.read_text(encoding="utf-8")
    assert "## 冻结的线（第 7 期 5.7，TRADING-2565-137）" in readme
    assert "frozen_lines.jsonl" in readme and "2026-11-23" in readme
    assert "`forward_frozen:`" in readme
    records = RECORDS.read_text(encoding="utf-8")
    assert "## 冻结的线（第 7 期 5.7）" in records
    assert "| F0010 |" in records and "2026-11-23" in records and "−43.84%" in records
    charter = CHARTER.read_text(encoding="utf-8")
    assert "冻进去的线比表里晚一个收盘成交" not in charter
    assert "卖出和表里同一个收盘" in charter and "买进比表里晚一个收盘" in charter
    assert "TRADING-2565-137" in REQUIREMENTS.read_text(encoding="utf-8")
    assert "forward_frozen" in SYSTEM_FLOW.read_text(encoding="utf-8")


# The walk and its decisions -----------------------------------------------------------------------


@pytest.mark.parametrize("scenario", sorted(POSTS))
def test_rows_follow_the_written_rule_in_every_scenario(
    appended: Callable[[str], Path], scenario: str
) -> None:
    ledger = appended(scenario) / "forward"
    rows = _rows(ledger)
    decisions, _ = _decisions(PATHS[scenario], END)
    expected = _oracle(scenario, ff.FROZEN_START, END)
    assert [r["date"] for r in rows] == [
        d.isoformat() for d in fl.trading_days(ff.FROZEN_START, END)
    ]
    for row, (trade, w, r) in zip(rows, expected, strict=True):
        assert row["rule_actions"] == decisions.get(row["date"], []), row["date"]
        assert fl._equal(row["trade_frozen"], trade), row["date"]
        assert fl._equal(row["exp_frozen"], w), row["date"]
        assert fl._equal(row["ret_frozen"], r), row["date"]
        assert row["line"] == "x2_s5_under_ladder"
    assert ff.verify(ledger) == []


def test_each_scenario_has_the_events_it_is_named_for(appended: Callable[[str], Path]) -> None:
    kinds = {}
    for scenario in POSTS:
        rows = _rows(appended(scenario) / "forward")
        kinds[scenario] = [
            (a["kind"], a.get("how") or a.get("reason") or a.get("level"))
            for row in rows
            for a in row["rule_actions"]
        ]
    assert kinds["recovery"] == [("fire", "X2"), ("fill", 0.9), ("full", "recovery")]
    assert kinds["ladder"] == [
        ("fire", "X2"),
        ("fill", 0.9),
        ("fill", 0.8),
        ("fill", 0.7),
        ("fill", 0.6),
        ("full", "ladder"),
    ]
    assert kinds["stop"] == [("fire", "X2"), ("full", "shift"), ("fire", "L"), ("full", "shift")]
    assert kinds["entry"] == [("fire", "X2"), ("full", "entry")]


def test_sales_trade_on_the_tables_close_and_purchases_one_close_later(
    appended: Callable[[str], Path],
) -> None:
    rows = {r["date"]: r for r in _rows(appended("recovery") / "forward")}
    _, trips = _decisions(PATHS["recovery"], END)
    trip = trips[-1]
    assert (trip["decision"], trip["sell"], trip["buy"]) == (
        "2026-12-16",
        "2026-12-17",
        "2027-02-25",
    )
    # The table sells at the close after X2 fires; so does the ledger.
    assert rows["2026-12-17"]["trade_frozen"] == 0.0 and rows["2026-12-18"]["exp_frozen"] == 0.0
    assert rows["2026-12-17"]["exp_frozen"] == 1.0
    # The table buys the tranche and the rest on the deciding close; the ledger one close later.
    assert rows["2027-01-07"]["trade_frozen"] is None
    assert rows["2027-01-08"]["trade_frozen"] == pytest.approx(rows["2027-01-07"]["rule_frac"])
    assert rows["2027-01-08"]["exp_frozen"] == 0.0
    assert rows["2027-02-25"]["trade_frozen"] is None
    assert rows["2027-02-26"]["trade_frozen"] == 1.0 and rows["2027-03-01"]["exp_frozen"] == 1.0


def test_rule_frac_is_the_cost_free_share_after_the_tranches(
    appended: Callable[[str], Path],
) -> None:
    rows = _rows(appended("ladder") / "forward")
    _, trips = _decisions(PATHS["ladder"], END)
    for row in rows:
        day = date.fromisoformat(row["date"])
        assert row["rule_frac"] == pytest.approx(_share(PATHS["ladder"], trips, day), abs=1e-12)
    by_day = {r["date"]: r for r in rows}
    assert 0.0 < by_day["2026-12-30"]["rule_frac"] < by_day["2027-01-15"]["rule_frac"] < 1.0
    assert by_day["2026-12-29"]["rule_frac"] == 0.0 and by_day["2027-02-23"]["rule_frac"] == 1.0
    # The last tranche buys the rest on its close: a fill and a full purchase, target one.
    assert by_day["2027-02-24"]["trade_frozen"] == 1.0


def test_a_fire_on_the_last_close_is_recorded_that_day(
    built: dict[str, Path], tmp_path: Path, synthetic: None
) -> None:
    for scenario, day, reason in (("recovery", "2026-12-16", "X2"), ("stop", "2027-02-11", "L")):
        daily, ledger = _copy(built, scenario, tmp_path / reason)
        _truncate_main(ledger, date.fromisoformat(day))
        rows = _append(daily, ledger)
        assert rows[-1]["date"] == day
        assert rows[-1]["rule_actions"] == [{"kind": "fire", "reason": reason}]
        assert rows[-1]["phase"] == "fired" and rows[-1]["rule_frac"] == 1.0
        _extend_main(ledger, scenario, fl.trading_days(date.fromisoformat(day), END)[3])
        more = _append(daily, ledger)
        assert more[0]["phase"] == "out" and more[0]["sale_date"] == more[0]["date"]
        assert more[0]["trade_frozen"] == 0.0 and more[0]["rule_frac"] == 0.0
        assert ff.verify(ledger) == []


def test_closes_after_the_row_are_never_used(
    built: dict[str, Path],
    appended: Callable[[str], Path],
    tmp_path: Path,
    synthetic: None,
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    crash = PATHS["recovery"].copy()
    crash[crash.index > pd.Timestamp("2026-12-11")] *= 0.5
    # Each capture from 12-01 to 12-11 also holds a halved price for every later day.
    for day in fl.trading_days(date(2026, 12, 1), date(2026, 12, 11)):
        _write_capture(daily, day, crash, through=END)
    _truncate_main(ledger, date(2026, 12, 11))
    rows = {r["date"]: r for r in _append(daily, ledger)}
    clean = {r["date"]: r for r in _rows(appended("recovery") / "forward")}
    for day in fl.trading_days(date(2026, 12, 1), date(2026, 12, 11)):
        key = day.isoformat()
        for field in ("rule_actions", "phase", "close_vs_high", "close_vs_line", "exp_frozen"):
            assert rows[key][field] == clean[key][field], (key, field)


def test_the_state_fields_describe_each_phase(appended: Callable[[str], Path]) -> None:
    rows = {r["date"]: r for r in _rows(appended("stop") / "forward")}
    qqq = PATHS["stop"]

    def close(day: str) -> float:
        return float(qqq[pd.Timestamp(day)])

    first = rows["2026-11-23"]
    assert first["phase"] == "in" and first["floor_date"] == "2026-03-30"
    assert first["close_vs_floor"] == pytest.approx(close("2026-11-23") / close("2026-03-30") - 1)
    high = max(
        (d for d in fl.trading_days(date(2026, 5, 14), date(2026, 11, 23))),
        key=lambda d: (close(d.isoformat()), -d.toordinal()),
    )
    assert first["high_date"] == high.isoformat()
    assert first["close_vs_high"] == pytest.approx(
        close("2026-11-23") / close(high.isoformat()) - 1
    )
    out = rows["2027-01-20"]
    assert out["phase"] == "out" and out["sale_date"] == "2027-01-13"
    assert out["filled_levels"] == [] and out["close_vs_x"] == pytest.approx(
        close("2027-01-20") / close("2027-01-13") - 1
    )
    assert out["high_date"] is None and out["floor_date"] is None
    shifted = rows["2027-02-04"]
    assert shifted["phase"] == "in" and shifted["floor_date"] == "2027-01-28"
    assert shifted["rule_actions"] == [{"kind": "full", "how": "shift", "floor_date": "2027-01-28"}]
    # X2's high is measured from the full purchase: on its own close, the high is that close.
    assert shifted["high_date"] == "2027-02-04" and shifted["close_vs_high"] == 0.0
    line = ec.moving_average(
        [float(v) for v in qqq[(qqq.index >= "2024-01-02") & (qqq.index <= "2027-02-04")]]
    )[-1]
    assert line is not None
    assert shifted["close_vs_line"] == pytest.approx(close("2027-02-04") / line - 1, abs=1e-12)


# The ledger's exposure ---------------------------------------------------------------------------


def test_the_exposure_drifts_between_trades_and_zero_and_one_stay() -> None:
    assert ff.drift(0.25, 0.04) == pytest.approx(0.25 * 1.04 / (1 + 0.25 * 0.04))
    assert ff.drift(0.0, 0.04) == 0.0 and ff.drift(1.0, None) == 1.0
    assert ff.drift(0.25, None) is None and ff.drift(None, 0.01) is None


def test_a_missing_return_empties_a_partial_line_until_the_next_trade(
    built: dict[str, Path], tmp_path: Path, synthetic: None
) -> None:
    daily, ledger = _copy(built, "ladder", tmp_path)
    gone = (date(2027, 1, 4), date(2026, 12, 3))  # out with one tranche; fully invested
    (ledger / fl.LEDGER_FILE).unlink()
    through = date(2027, 1, 22)
    _main_through(ledger, PATHS["ladder"], through, missing=gone)
    rows = {r["date"]: r for r in _append(daily, ledger)}
    expected = dict(
        zip(
            [d.isoformat() for d in fl.trading_days(ff.FROZEN_START, through)],
            _oracle("ladder", ff.FROZEN_START, through, missing=gone),
            strict=True,
        )
    )
    for day, (_trade, w, r) in expected.items():
        assert fl._equal(rows[day]["exp_frozen"], w), day
        assert fl._equal(rows[day]["ret_frozen"], r), day
    assert rows["2027-01-04"]["ret_frozen"] is None and rows["2027-01-04"]["notes"]
    assert rows["2027-01-05"]["exp_frozen"] is None  # partial: cannot drift without q
    assert rows["2027-01-19"]["exp_frozen"] is None  # still empty before the next trade
    assert rows["2027-01-20"]["exp_frozen"] is not None  # the 01-15 tranche traded on 01-19
    assert rows["2026-12-04"]["exp_frozen"] == 1.0  # one does not drift
    assert ff.verify(ledger) == []


@pytest.mark.parametrize(
    ("start", "w", "trade"),
    [
        (date(2026, 11, 23), 1.0, None),  # fully invested since the printed shift purchase
        (date(2026, 12, 18), 0.0, None),  # the sale traded on 12-17
        (date(2027, 1, 8), 0.0, "tranche"),  # the 0.9 tranche decided on 01-07 trades today
        (date(2027, 1, 12), "drift", None),  # holding the tranche, drifting
    ],
)
def test_the_first_row_simulates_the_ledger_from_the_anchor(
    built: dict[str, Path],
    tmp_path: Path,
    synthetic: None,
    monkeypatch: pytest.MonkeyPatch,
    start: date,
    w: Any,
    trade: Any,
) -> None:
    monkeypatch.setattr(ff, "FROZEN_START", start)
    daily, ledger = _copy(built, "recovery", tmp_path)
    _truncate_main(ledger, fl.trading_days(start, END)[2])
    rows = _append(daily, ledger)
    oracle_trade, oracle_w, _ = _oracle("recovery", start, END)[0]
    assert rows[0]["date"] == start.isoformat()
    assert fl._equal(rows[0]["exp_frozen"], oracle_w)
    assert fl._equal(rows[0]["trade_frozen"], oracle_trade)
    if w == "drift":
        assert 0.0 < rows[0]["exp_frozen"] < 1.0
    else:
        assert rows[0]["exp_frozen"] == w
    assert (rows[0]["trade_frozen"] is None) == (trade is None)
    assert rows[0]["prev_sha256"] is None and ff.verify(ledger) == []


def test_later_rows_continue_from_the_written_rows(
    built: dict[str, Path], appended: Callable[[str], Path], tmp_path: Path, synthetic: None
) -> None:
    once = appended("recovery") / "forward"
    twice_daily, twice = _copy(built, "recovery", tmp_path / "twice")
    _truncate_main(twice, date(2027, 1, 5))
    _append(twice_daily, twice)
    _extend_main(twice, "recovery", END)
    _append(twice_daily, twice)
    assert (once / ff.FROZEN_FILE).read_bytes() == (twice / ff.FROZEN_FILE).read_bytes()


def test_a_proportional_rescale_of_old_prices_changes_nothing(
    built: dict[str, Path], tmp_path: Path, synthetic: None
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    _truncate_main(ledger, date(2027, 1, 5))
    _append(daily, ledger)
    before = (ledger / ff.FROZEN_FILE).read_bytes()
    rescaled = PATHS["recovery"].copy()
    rescaled[rescaled.index < pd.Timestamp("2027-01-04")] *= 0.997  # a dividend adjustment
    for day in fl.trading_days(date(2027, 1, 6), END):
        _write_capture(daily, day, rescaled.round(10))
    _extend_main(ledger, "recovery", date(2027, 1, 29))
    rows = _append(daily, ledger)
    assert (ledger / ff.FROZEN_FILE).read_bytes().startswith(before)
    assert rows and ff.verify(ledger) == []


# Which closes, and the two checks ----------------------------------------------------------------


def test_a_row_whose_own_file_cannot_walk_uses_the_next_file_that_can(
    built: dict[str, Path],
    appended: Callable[[str], Path],
    tmp_path: Path,
    synthetic: None,
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    recovery = PATHS["recovery"]
    _write_capture(daily, date(2026, 12, 2), recovery, drop=(date(2025, 8, 5),))  # a gap
    _write_capture(daily, date(2026, 12, 3), recovery, through=date(2026, 12, 2))  # no 12-03
    (daily / _file("2026-12-08")).unlink()  # no file at all
    _truncate_main(ledger, date(2026, 12, 10))
    rows = {r["date"]: r for r in _append(daily, ledger)}
    clean = {r["date"]: r for r in _rows(appended("recovery") / "forward")}
    # 12-03's own file still walks to 12-02; 12-08 falls to 12-09.
    moved = {"2026-12-02": "2026-12-03", "2026-12-03": "2026-12-04", "2026-12-08": "2026-12-09"}
    assert list(rows) == [
        d.isoformat() for d in fl.trading_days(ff.FROZEN_START, date(2026, 12, 10))
    ]
    for day, row in rows.items():
        used = _file(moved.get(day, day))
        assert row["price_file"] == _file(day) and row["rule_price_file"] == used, day
        if day in moved:
            assert any(used in note for note in row["notes"]), day
        else:
            assert row["notes"] == clean[day]["notes"], day
        same = ff.ROW_KEYS - {"rule_price_file", "notes", "prev_sha256"}
        if day in ("2026-12-03", "2026-12-08"):
            assert row["sgov_tr"] is None  # SGOV comes from the row's own file only
            same -= {"sgov_tr"}
        for key in same:
            assert row[key] == clean[day][key], (day, key)
    assert ff.verify(ledger) == [] and ff.check_walk(ledger, daily) == []
    for day in moved:
        assert ff.check_day(ledger, daily, date.fromisoformat(day)) == [], day


def test_rows_wait_for_a_later_file_when_none_can_walk_yet(
    built: dict[str, Path], tmp_path: Path, synthetic: None
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    bad = date(2026, 12, 2)
    _write_capture(daily, bad, PATHS["recovery"], drop=(date(2025, 8, 5),))
    _truncate_main(ledger, bad)
    with pytest.raises(ff.FrozenLinesError, match="2026-12-02"):
        _append(daily, ledger)
    rows = _rows(ledger)
    assert rows[-1]["date"] == "2026-12-01" and ff.verify(ledger) == []
    with pytest.raises(ff.FrozenLinesError):  # still nothing that can walk: nothing new
        _append(daily, ledger)
    assert _rows(ledger) == rows
    _extend_main(ledger, "recovery", date(2026, 12, 3))
    more = _append(daily, ledger)
    assert [r["date"] for r in more] == ["2026-12-02", "2026-12-03"]
    assert [r["rule_price_file"] for r in more] == [_file("2026-12-03")] * 2
    assert ff.verify(ledger) == []


def test_check_day_walks_the_rule_price_file_and_notes_a_repaired_own_file(
    built: dict[str, Path], tmp_path: Path, synthetic: None
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    day = date(2026, 12, 2)
    _write_capture(daily, day, PATHS["recovery"], drop=(date(2025, 8, 5),))
    _truncate_main(ledger, date(2026, 12, 4))
    _append(daily, ledger)
    lines: list[str] = []
    assert ff.check_day(ledger, daily, day, lines.append) == [] and lines == []
    _write_capture(daily, day, PATHS["recovery"])  # the own file is whole again: a notice only
    assert ff.check_day(ledger, daily, day, lines.append) == []
    assert len(lines) == 1 and lines[0].startswith("forward_frozen: ") and "提示" in lines[0]
    assert ff.after_check(ledger, day, daily, lines.append) is True
    # The file it names must still walk to that day, and walk to what is written.
    _write_capture(daily, date(2026, 12, 3), PATHS["recovery"], drop=(date(2025, 8, 5),))
    assert ff.check_day(ledger, daily, day) != []
    moved = _bumped(PATHS["recovery"], "2026-12-02", 1.01)
    _write_capture(daily, date(2026, 12, 3), moved)
    assert ff.check_day(ledger, daily, day) != []
    assert ff.after_check(ledger, day, daily, lines.append) is False


def test_check_day_notes_an_sgov_return_that_came_back(
    built: dict[str, Path], tmp_path: Path, synthetic: None
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    (daily / _file("2026-12-08")).unlink()
    _truncate_main(ledger, date(2026, 12, 10))
    _append(daily, ledger)
    day = date(2026, 12, 8)
    assert next(r for r in _rows(ledger) if r["date"] == "2026-12-08")["sgov_tr"] is None
    _write_capture(daily, day, PATHS["recovery"])  # the own file is back, SGOV and all
    lines: list[str] = []
    # Two notices, no failure: the own file walks again, and its SGOV return reads again.
    assert ff.check_day(ledger, daily, day, lines.append) == []
    assert len(lines) == 2, lines
    assert all(line.startswith("forward_frozen: ") and "提示" in line for line in lines)


def test_a_later_rule_file_named_in_the_notes_is_kept(written: tuple[Path, Path]) -> None:
    daily, ledger = written
    seq = _seq(ledger, "2026-12-01")
    _edit(ledger, seq, _rule_file("2026-12-02"))
    assert ff.verify(ledger) == []
    lines: list[str] = []
    assert ff.check_day(ledger, daily, date(2026, 12, 1), lines.append) == []
    assert len(lines) == 1 and "提示" in lines[0]  # its own file walks too


def test_walking_needs_closes_from_walk_from(
    built: dict[str, Path], tmp_path: Path, synthetic: None
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    for day in fl.trading_days(CAPTURE_FROM, END):
        _write_capture(daily, day, PATHS["recovery"], drop=(date(2024, 1, 2),))
    with pytest.raises(ff.FrozenLinesError, match="2024-01-02"):
        _append(daily, ledger)
    assert _rows(ledger) == []


@pytest.mark.parametrize("scenario", sorted(POSTS))
def test_walking_to_any_close_keeps_every_earlier_decision(scenario: str, synthetic: None) -> None:
    """Prefix consistency: the walk cut at t decides exactly what the whole walk decides up to t.

    This covers the last-close fire (X2 and L) and is what check B relies on with real files.
    """
    qqq = PATHS[scenario]
    kept = qqq[qqq.index >= pd.Timestamp(ff.WALK_FROM)]
    dates = [stamp.date() for stamp in kept.index]
    close = [float(value) for value in kept]
    whole = ff.walk(dates, close)
    _, trips = _decisions(qqq, dates[-1])
    for i, day in enumerate(dates):
        if not ff.FROZEN_START <= day <= END:
            continue
        key = day.isoformat()
        part = ff.walk(dates[: i + 1], close[: i + 1])
        assert part.actions == {d: a for d, a in whole.actions.items() if d <= key}, key
        fired = any(a["kind"] == "fire" for a in whole.actions.get(key, []))
        trip = next(
            (t for t in trips if t["sell"] <= key and (t["buy"] is None or key < t["buy"])), None
        )
        assert part.state["phase"] == ("fired" if fired else "out" if trip else "in"), key
        assert part.state["sale_date"] == (trip["sell"] if trip else None), key
        assert part.state["rule_frac"] == pytest.approx(_share(qqq, trips, day), abs=1e-12), key


def test_guard_a_runs_on_every_rows_file(
    built: dict[str, Path], tmp_path: Path, synthetic: None
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    _truncate_main(ledger, date(2026, 11, 25))
    _append(daily, ledger)
    before = (ledger / ff.FROZEN_FILE).read_bytes()
    # From 12-01 the files move the printed decision close: X2 fires a close later, before the
    # snapshot. 11-27 and 11-30 still pass check A, but nothing of this append is written.
    moved = _bumped(PATHS["recovery"], "2026-03-11", 1.01)
    for day in fl.trading_days(date(2026, 12, 1), date(2026, 12, 4)):
        _write_capture(daily, day, moved)
    _extend_main(ledger, "recovery", date(2026, 12, 4))
    with pytest.raises(ff.FrozenLinesError):
        _append(daily, ledger)
    assert (ledger / ff.FROZEN_FILE).read_bytes() == before


def _guard_fails(daily: Path, ledger: Path) -> None:
    with pytest.raises(ff.FrozenLinesError):
        _append(daily, ledger)
    assert not (ledger / ff.FROZEN_FILE).exists()


@pytest.mark.parametrize(
    "change",
    [
        {"decision": "2026-03-12"},
        {"sell": "2026-03-13"},
        {"buy": "2026-05-15"},
        {"how": "recovery"},
        {"floor_date": "2026-03-31"},
        {"fills": ((0.9, "2026-03-20"),)},
        {"reason": "L"},
    ],
)
def test_guard_a_wants_the_printed_trip_exactly(
    built: dict[str, Path],
    tmp_path: Path,
    synthetic: None,
    monkeypatch: pytest.MonkeyPatch,
    change: dict[str, Any],
) -> None:
    monkeypatch.setattr(ff, "PRINTED", ({**PRINTED[0], **change},))
    _guard_fails(*_copy(built, "recovery", tmp_path))


def test_guard_a_wants_one_trip_no_more_no_less(
    built: dict[str, Path], tmp_path: Path, synthetic: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ff, "PRINTED", ())
    _guard_fails(*_copy(built, "recovery", tmp_path / "none"))
    monkeypatch.setattr(ff, "PRINTED", (*PRINTED, *PRINTED))
    _guard_fails(*_copy(built, "recovery", tmp_path / "two"))


def test_guard_a_wants_the_snapshot_end_state(
    built: dict[str, Path], tmp_path: Path, synthetic: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ff, "PRINTED_FLOOR", date(2026, 3, 31))
    _guard_fails(*_copy(built, "recovery", tmp_path / "floor"))
    monkeypatch.setattr(ff, "PRINTED_FLOOR", PRINTED_FLOOR)
    # Cut the snapshot inside the printed trip: the walk is out of the market at its end.
    monkeypatch.setattr(ff, "SNAPSHOT_END", date(2026, 4, 20))
    monkeypatch.setattr(ff, "PRINTED", ({**PRINTED[0], "buy": None, "how": "none"},))
    _guard_fails(*_copy(built, "recovery", tmp_path / "open"))


def test_guard_a_checks_the_anchors_own_trip(
    built: dict[str, Path], tmp_path: Path, synthetic: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ff, "ANCHOR_AT", ANCHOR_AT + 0.002)
    _guard_fails(*_copy(built, "recovery", tmp_path / "at"))
    monkeypatch.setattr(ff, "ANCHOR_AT", ANCHOR_AT + 0.0009)
    near_daily, near_ledger = _copy(built, "recovery", tmp_path / "close")
    _truncate_main(near_ledger, date(2026, 11, 25))
    assert _append(near_daily, near_ledger)
    monkeypatch.setattr(ff, "ANCHOR_AT", ANCHOR_AT)
    # A later "back at X" close than the anchor: move the anchor's sale to a lower close.
    monkeypatch.setattr(ff, "ANCHOR_SALE", date(2025, 3, 6))
    monkeypatch.setattr(
        ff,
        "ANCHOR_AT",
        float(
            PATHS["recovery"][pd.Timestamp(ANCHOR_FILL)]
            / PATHS["recovery"][pd.Timestamp("2025-03-06")]
        ),
    )
    _guard_fails(*_copy(built, "recovery", tmp_path / "back"))


def test_guard_a_refuses_a_signal_before_the_anchor(
    built: dict[str, Path], tmp_path: Path, synthetic: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = es.shifts
    early = DAYS.index(date(2025, 4, 15)) - DAYS.index(ff.WALK_FROM)

    def with_an_early_move(close: Any, line: Any, k: int, start: int) -> list[es.Shift]:
        found = real(close, line, k, start)
        if len(close) > early:
            found = sorted(
                [*found, es.Shift(early - 10, early - 6, early - 12, early)],
                key=lambda move: move.entry,
            )
        return found

    monkeypatch.setattr(es, "shifts", with_an_early_move)
    _guard_fails(*_copy(built, "recovery", tmp_path))


def test_guard_b_keeps_every_written_decision(
    built: dict[str, Path], tmp_path: Path, synthetic: None
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    _truncate_main(ledger, date(2027, 1, 5))
    _append(daily, ledger)
    before = (ledger / ff.FROZEN_FILE).read_bytes()
    # A later file moves one past close (not a proportional rescale): X2 no longer fires 12-16.
    moved = _bumped(PATHS["recovery"], "2026-12-16", 1.03)
    for day in fl.trading_days(date(2027, 1, 6), END):
        _write_capture(daily, day, moved)
    _extend_main(ledger, "recovery", END)
    with pytest.raises(ff.FrozenLinesError):
        _append(daily, ledger)
    assert (ledger / ff.FROZEN_FILE).read_bytes() == before


def test_guard_b_keeps_the_first_rows_trade(
    built: dict[str, Path], tmp_path: Path, synthetic: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ff, "FROZEN_START", date(2027, 1, 8))
    daily, ledger = _copy(built, "recovery", tmp_path)
    _truncate_main(ledger, date(2027, 1, 8))
    rows = _append(daily, ledger)
    assert rows[0]["trade_frozen"] is not None  # the 01-07 tranche, decided before the start
    # Later files fill the 0.9 tranche on 01-06: 01-08 still decides nothing, but nothing trades
    # on it (the tranche traded on 01-07, before the start).
    moved = _bumped(PATHS["recovery"], "2027-01-06", 0.99)
    for day in fl.trading_days(date(2027, 1, 11), END):
        _write_capture(daily, day, moved)
    _extend_main(ledger, "recovery", END)
    with pytest.raises(ff.FrozenLinesError):
        _append(daily, ledger)
    assert len(_rows(ledger)) == 1


def test_guard_b_checks_rows_computed_earlier_in_the_same_append(
    built: dict[str, Path], tmp_path: Path, synthetic: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ff, "FROZEN_START", date(2027, 1, 4))
    daily, ledger = _copy(built, "recovery", tmp_path)
    # From 01-08 the files fill the 0.9 tranche on 01-06, not on 01-07 as 01-07's own file did.
    moved = _bumped(PATHS["recovery"], "2027-01-06", 0.99)
    for day in fl.trading_days(date(2027, 1, 8), END):
        _write_capture(daily, day, moved)
    _truncate_main(ledger, date(2027, 1, 12))
    with pytest.raises(ff.FrozenLinesError):
        _append(daily, ledger)
    assert not (ledger / ff.FROZEN_FILE).exists()


# Hanging on the main ledger, the chain, the start ------------------------------------------------


def test_rows_start_at_the_start_and_hang_on_the_main_rows(
    appended: Callable[[str], Path],
) -> None:
    ledger = appended("recovery") / "forward"
    rows = _rows(ledger)
    pairs = zip(fl.read_ledger(ledger), fl._ledger_lines(ledger), strict=True)
    main = {r["date"]: (r, raw) for r, raw in pairs}
    assert rows[0]["date"] == "2026-11-23" and rows[0]["seq"] == 1
    previous: bytes | None = None
    for row in rows:
        source, raw = main[row["date"]]
        assert row["ledger_seq"] == source["seq"]
        assert row["ledger_sha256"] == chain_digest(raw)
        assert row["qqq_tr"] == source["qqq_tr"] and row["price_file"] == source["price_file"]
        assert row["prev_sha256"] == (None if previous is None else chain_digest(previous))
        assert row["code_commit"] == COMMIT and row["code_modified"] is False
        assert row.keys() == ff.ROW_KEYS
        previous = json.dumps(
            row, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    day = date.fromisoformat(rows[3]["date"])
    assert rows[3]["sgov_tr"] == pytest.approx(_q(SGOV.round(10), day))


def test_crlf_rows_are_kept_and_chained_as_lf(recovery: tuple[Path, Path]) -> None:
    daily, ledger = recovery
    _truncate_main(ledger, date(2026, 12, 4))
    _append(daily, ledger)
    path = ledger / ff.FROZEN_FILE
    # CRLF, and the last row without its newline: the next rows start on a line of their own.
    path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n").rstrip(b"\r\n"))
    _extend_main(ledger, "recovery", date(2026, 12, 11))
    _append(daily, ledger)
    assert ff.verify(ledger) == []
    assert len(ff.read_frozen(ledger)) == len(fl.trading_days(ff.FROZEN_START, date(2026, 12, 11)))


def test_nothing_is_written_or_read_before_the_start(
    built: dict[str, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No synthetic guard: before the start nothing reads prices or runs the checks.
    daily, ledger = _copy(built, "recovery", tmp_path)
    _truncate_main(ledger, date(2026, 11, 20))
    shutil.rmtree(daily)
    lines: list[str] = []
    assert ff.after_append(ledger, daily, PROVENANCE, lines.append) is True
    assert not (ledger / ff.FROZEN_FILE).exists()
    assert any("2026-11-23" in line and "还没有行" in line for line in lines)
    assert ff.after_check(ledger, None, daily, lines.append) is True


def test_appending_needs_a_clean_commit(recovery: tuple[Path, Path]) -> None:
    daily, ledger = recovery
    with pytest.raises(ff.FrozenLinesError):
        ff.append(ledger, daily, provenance={**PROVENANCE, "code_modified": True})
    with pytest.raises(ff.FrozenLinesError):
        ff.append(ledger, daily, provenance={**PROVENANCE, "commit": "abc"})
    _truncate_main(ledger, date(2026, 11, 20))  # nothing to write: still refused
    with pytest.raises(ff.FrozenLinesError):
        ff.append(ledger, daily, provenance={**PROVENANCE, "code_modified": True})
    assert not (ledger / ff.FROZEN_FILE).exists()


def test_appending_refuses_a_frozen_file_that_fails_check(recovery: tuple[Path, Path]) -> None:
    daily, ledger = recovery
    _truncate_main(ledger, date(2026, 12, 4))
    _append(daily, ledger)
    _edit(ledger, 2, _shift("ret_frozen", 0.01))  # chained, but not exp_frozen * qqq_tr
    before = (ledger / ff.FROZEN_FILE).read_bytes()
    _extend_main(ledger, "recovery", date(2026, 12, 8))
    with pytest.raises(ff.FrozenLinesError):
        _append(daily, ledger)
    assert (ledger / ff.FROZEN_FILE).read_bytes() == before


def test_the_frozen_file_may_lag_the_main_ledger(recovery: tuple[Path, Path]) -> None:
    daily, ledger = recovery
    _truncate_main(ledger, date(2026, 12, 4))
    _append(daily, ledger)
    _extend_main(ledger, "recovery", END)
    assert ff.verify(ledger) == []
    text = "\n".join(ff.render_section(fl.read_ledger(ledger), ff.read_frozen(ledger)))
    lag = len(fl.trading_days(date(2026, 12, 7), END))
    assert f"冻结线落后主账本 {lag} 行。" in text


# check -------------------------------------------------------------------------------------------


@pytest.fixture()
def written(appended: Callable[[str], Path], tmp_path: Path, synthetic: None) -> tuple[Path, Path]:
    return _copy({"stop": appended("stop")}, "stop", tmp_path)


def _edit(
    ledger: Path, seq: int, change: Callable[[dict[str, Any]], None], *, chain: bool = True
) -> None:
    """Change one row; by default re-chain the file so only the change itself is wrong."""
    path = ledger / ff.FROZEN_FILE
    lines = path.read_bytes().splitlines()
    row = json.loads(lines[seq - 1])
    change(row)
    lines[seq - 1] = _dumps(row)
    path.write_bytes(b"\n".join(lines) + b"\n")
    if chain:
        _rechain(ledger)


def _dumps(row: dict[str, Any]) -> bytes:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _rechain(ledger: Path) -> None:
    path = ledger / ff.FROZEN_FILE
    rows = [json.loads(line) for line in path.read_bytes().splitlines()]
    previous = None
    out = []
    for row in rows:
        row["prev_sha256"] = previous
        out.append(_dumps(row))
        previous = chain_digest(out[-1])
    path.write_bytes(b"\n".join(out) + b"\n")


def _seq(ledger: Path, day: str) -> int:
    return int(next(r["seq"] for r in _rows(ledger) if r["date"] == day))


def _set(key: str, value: Any) -> Callable[[dict[str, Any]], None]:
    def change(row: dict[str, Any]) -> None:
        row[key] = value

    return change


def _rule_file(day: str) -> Callable[[dict[str, Any]], None]:
    def change(row: dict[str, Any]) -> None:
        row["rule_price_file"] = _file(day)
        row["notes"] = [*row["notes"], f"规则用 {_file(day)}"]

    return change


def _shift(key: str, delta: float) -> Callable[[dict[str, Any]], None]:
    def change(row: dict[str, Any]) -> None:
        row[key] = row[key] + delta

    return change


def _qqq_tr(value: float) -> Callable[[dict[str, Any]], None]:
    """A different QQQ return, with the line's return moved to match: only the hang is wrong."""

    def change(row: dict[str, Any]) -> None:
        row["qqq_tr"] = value
        row["ret_frozen"] = fl._product(row["exp_frozen"], value)

    return change


def _exposure(value: float) -> Callable[[dict[str, Any]], None]:
    """A different exposure, with the line's return moved to match: only the drift is wrong."""

    def change(row: dict[str, Any]) -> None:
        row["exp_frozen"] = value
        row["ret_frozen"] = fl._product(value, row["qqq_tr"])

    return change


def _both_files(value: str) -> Callable[[dict[str, Any]], None]:
    def change(row: dict[str, Any]) -> None:
        row["price_file"] = row["rule_price_file"] = value

    return change


def _as_in(row: dict[str, Any]) -> None:
    """Every field of a fully invested close, but no full purchase decided it."""
    row.update(
        phase="in",
        rule_frac=1.0,
        sale_date=None,
        filled_levels=None,
        close_vs_x=None,
        high_date=row["date"],
        close_vs_high=0.0,
        floor_date=None,
        close_vs_floor=None,
    )


def _as_out(row: dict[str, Any]) -> None:
    """Every field of a close out of the market since the close before, but no fire."""
    row.update(
        phase="out",
        rule_frac=0.0,
        sale_date="2026-11-30",
        filled_levels=[],
        close_vs_x=0.0,
        high_date=None,
        close_vs_high=None,
        floor_date=None,
        close_vs_floor=None,
    )


def _later_file_odd_note(row: dict[str, Any]) -> None:
    """A later row's file named as the rule file, with a note that is not a string."""
    row["rule_price_file"] = _file("2026-12-02")
    row["notes"] = [1]


FIRE_X2 = {"kind": "fire", "reason": "X2"}
FILL_09 = {"kind": "fill", "level": 0.9, "share": 0.1}
SHIFT_0128 = {"kind": "full", "how": "shift", "floor_date": "2027-01-28"}
BROKEN: list[tuple[str, Callable[[dict[str, Any]], None]]] = [
    ("2026-12-01", _shift("ret_frozen", 1e-9)),
    ("2026-12-01", _set("exp_frozen", 0.9)),
    ("2026-12-01", _exposure(0.9)),
    ("2026-12-01", _set("trade_frozen", 1.0)),
    ("2027-01-13", _set("trade_frozen", None)),  # the 01-12 fire trades on the sale close
    ("2027-02-05", _set("trade_frozen", 0.0)),  # the shift on 02-04 targets one
    ("2027-02-05", _set("exp_frozen", 1.0)),  # still empty: traded on 02-05, held from 02-08
    ("2027-01-20", _set("rule_frac", 0.5)),
    ("2027-01-20", _set("phase", "in")),
    ("2027-01-20", _set("sale_date", "2027-01-14")),
    ("2027-01-20", _set("filled_levels", [0.8])),
    ("2027-01-20", _set("close_vs_x", None)),
    ("2027-01-20", _set("high_date", "2027-01-05")),
    ("2026-12-01", _set("high_date", None)),
    ("2026-12-01", _set("floor_date", None)),
    ("2026-12-01", _set("sale_date", "2026-11-30")),
    ("2026-12-01", _set("rule_frac", 0.99)),
    ("2026-12-01", _set("phase", "out")),
    ("2026-12-01", _set("phase", "fired")),
    ("2026-12-01", _set("close_vs_line", None)),
    ("2026-12-01", _set("line", "x2_s5_under")),
    ("2026-12-01", _set("rule_actions", [{"kind": "fire", "reason": "X3"}])),
    ("2027-01-12", _set("rule_actions", [{"kind": "fire", "reason": "X3"}])),
    ("2026-12-01", _set("rule_actions", [{"kind": "fill", "level": 0.9, "share": 0.2}])),
    ("2026-12-01", _set("rule_actions", [{"kind": "full", "how": "shift", "floor_date": None}])),
    (
        "2026-12-01",
        _set("rule_actions", [{"kind": "full", "how": "entry", "floor_date": "2026-11-30"}]),
    ),
    ("2026-12-01", _set("rule_actions", [{"kind": "buy"}])),
    ("2026-12-01", _set("rule_actions", {})),
    ("2027-01-12", _set("rule_actions", [])),  # the X2 close must show the fire
    ("2027-01-12", _set("rule_actions", [FIRE_X2, FILL_09])),  # a fire comes alone
    ("2027-02-04", _set("rule_actions", [SHIFT_0128, FILL_09])),  # fills come before a full
    ("2027-01-13", _set("phase", "fired")),  # the close after a fire is the sale close
    ("2027-01-13", _set("sale_date", "2027-01-12")),
    ("2027-02-12", _set("phase", "in")),  # out only ends with a full purchase
    ("2027-01-20", _as_in),  # out only ends with a full purchase
    ("2026-12-01", _as_out),  # in only turns out after a fire
    ("2027-01-20", _set("rule_actions", [{"kind": "full", "how": "recovery", "floor_date": None}])),
    ("2026-12-01", _set("notes", "x")),
    ("2026-12-01", _set("code_commit", "abc")),
    ("2026-12-01", _set("code_modified", True)),
    ("2026-12-01", _set("schema_version", "forward_frozen.v0")),
    ("2026-12-01", _set("qqq_tr", 0.5)),
    ("2026-12-01", _qqq_tr(0.5)),
    ("2026-12-01", _set("price_file", "x.csv")),
    ("2026-12-01", _both_files("x.csv")),
    ("2026-12-01", _set("rule_price_file", "x.csv")),
    ("2026-12-01", _set("rule_price_file", _file("2026-12-02"))),  # later, but no note names it
    ("2026-12-01", _rule_file("2026-11-30")),  # an earlier row's file, even with a note
    ("2026-12-01", _later_file_odd_note),  # reported, never raised
    ("2026-12-01", _set("ledger_seq", 1)),
    ("2026-12-01", _set("ledger_sha256", "0" * 64)),
    ("2026-12-01", _set("seq", 99)),
    ("2026-12-01", _set("date", "2026-12-02")),
]


@pytest.mark.parametrize(("day", "change"), BROKEN)
def test_check_catches_each_broken_identity(
    written: tuple[Path, Path], day: str, change: Callable[[dict[str, Any]], None]
) -> None:
    _, ledger = written
    seq = _seq(ledger, day)
    _edit(ledger, seq, change)
    problems = ff.verify(ledger)
    # Caught on the changed row itself, not only through the row after it.
    assert problems and problems[0].startswith(f"{ff.FROZEN_FILE} line {seq}: "), problems


def test_check_catches_tranche_levels_out_of_order(
    appended: Callable[[str], Path], tmp_path: Path
) -> None:
    _, ledger = _copy({"ladder": appended("ladder")}, "ladder", tmp_path)
    seq = _seq(ledger, "2026-12-30")  # the 0.9 tranche
    _edit(ledger, seq, _set("filled_levels", [0.8]))
    problems = ff.verify(ledger)
    assert problems and problems[0].startswith(f"{ff.FROZEN_FILE} line {seq}: "), problems


def test_the_first_row_out_of_the_market_needs_a_sale_on_or_before_it(
    built: dict[str, Path], tmp_path: Path, synthetic: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ff, "FROZEN_START", date(2027, 1, 20))
    daily, ledger = _copy(built, "stop", tmp_path)
    _truncate_main(ledger, date(2027, 1, 22))
    rows = _append(daily, ledger)
    assert rows[0]["phase"] == "out" and rows[0]["sale_date"] == "2027-01-13"
    for value in ("2027-01-21", "x"):  # no row before it to compare with
        _edit(ledger, 1, _set("sale_date", value))
        problems = ff.verify(ledger)
        assert problems and problems[0].startswith(f"{ff.FROZEN_FILE} line 1: "), problems


@pytest.mark.parametrize("value", ["x", [1]], ids=["string", "list"])
@pytest.mark.parametrize("day", ["2026-12-01", "2027-01-20"])  # fully invested; out
@pytest.mark.parametrize("key", sorted(ff.ROW_KEYS))
def test_a_string_or_a_list_in_any_field_is_reported_never_raised(
    written: tuple[Path, Path], key: str, day: str, value: Any
) -> None:
    _, ledger = written
    seq = _seq(ledger, day)
    _edit(ledger, seq, _set(key, value), chain=key != "prev_sha256")
    problems = ff.verify(ledger)
    assert problems and problems[0].startswith(f"{ff.FROZEN_FILE} line {seq}: "), problems


def test_a_bad_cell_never_raises_from_the_commands(
    cli_ready: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    daily, ledger = cli_ready
    _append(daily, ledger)
    _edit(ledger, 3, _set("rule_frac", "1.0"))
    monkeypatch.setattr(fl, "append", lambda *a, **k: [])
    for args in (
        ["check", "--ledger-dir", str(ledger), "--daily-root", str(daily)],
        ["weekly", "--ledger-dir", str(ledger)],
        [
            "append",
            "--through",
            "2026-12-11",
            "--daily-root",
            str(daily),
            "--ledger-dir",
            str(ledger),
        ],
    ):
        result = _cli(*args)
        assert result.exit_code == 1 and isinstance(result.exception, SystemExit), args
        assert f"{ff.FROZEN_FILE} line 3: " in result.output and "Traceback" not in result.output


def test_frozen_rows_past_the_main_ledger_are_reported_never_raised(
    cli_ready: tuple[Path, Path],
) -> None:
    daily, ledger = cli_ready
    rows = _append(daily, ledger)
    _truncate_main(ledger, date.fromisoformat(rows[-3]["date"]))  # the main ledger went back
    problem = f"{ff.FROZEN_FILE} line {len(rows) - 1}: main ledger has no matching row"
    assert ff.verify(ledger) == [problem]
    result = _cli("check", "--ledger-dir", str(ledger))
    assert result.exit_code == 1 and isinstance(result.exception, SystemExit), result.output
    assert problem in result.output and "Traceback" not in result.output


def test_check_catches_extra_keys_a_missing_row_and_bad_json(written: tuple[Path, Path]) -> None:
    _, ledger = written
    path = ledger / ff.FROZEN_FILE
    original = path.read_bytes()
    _edit(ledger, 3, _set("extra", 1))
    assert ff.verify(ledger) != []
    path.write_bytes(original)
    _edit(ledger, 3, _set("prev_sha256", "0" * 64), chain=False)
    assert ff.verify(ledger) == [f"{ff.FROZEN_FILE} line 3: prev_sha256 does not match"]
    path.write_bytes(original)
    _edit(ledger, 3, _shift("ret_frozen", 0.01), chain=False)  # the next row's link breaks too
    assert ff.verify(ledger)[0].startswith(f"{ff.FROZEN_FILE} line 3:")
    lines = original.splitlines()
    path.write_bytes(b"\n".join(lines[:2] + lines[3:]) + b"\n")
    assert ff.verify(ledger) != []
    path.write_bytes(original + b"{not json\n")
    assert ff.verify(ledger) != []
    path.write_bytes(original)
    assert ff.verify(ledger) == []


def test_check_with_prices_runs_both_checks_and_the_first_rows_start(
    written: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    daily, ledger = written
    assert ff.check_walk(ledger, daily) == []
    monkeypatch.setattr(ff, "PRINTED", ({**PRINTED[0], "buy": "2026-05-15"},))
    assert ff.check_walk(ledger, daily) != []
    monkeypatch.setattr(ff, "PRINTED", PRINTED)
    last = _rows(ledger)[-1]
    # The X2 close no longer fires in the last file.
    moved = _bumped(PATHS["stop"], "2027-01-12", 1.05)
    _write_capture(daily, date.fromisoformat(last["date"]), moved)
    assert ff.check_walk(ledger, daily) != []
    lines: list[str] = []
    assert ff.after_check(ledger, None, daily, lines.append) is False
    assert lines and all(line.startswith("forward_frozen: ") for line in lines)
    _write_capture(daily, date.fromisoformat(last["date"]), PATHS["stop"])
    assert ff.check_walk(ledger, daily) == []
    # A file that is valid on its own but starts empty: only the simulated start catches it.
    for row in _rows(ledger):
        if row["date"] > "2027-01-13":
            break
        _edit(ledger, row["seq"], _set("exp_frozen", 0.0))
        _edit(ledger, row["seq"], _set("ret_frozen", 0.0 if row["qqq_tr"] is not None else None))
    _rechain(ledger)
    assert ff.verify(ledger) == []
    assert ff.check_walk(ledger, daily) != []
    assert ff.check_day(ledger, daily, ff.FROZEN_START) != []


@pytest.mark.parametrize(
    ("day", "key", "value"),
    [
        ("2027-01-20", "close_vs_x", 0.0),
        ("2026-12-01", "close_vs_high", 0.0),
        ("2026-12-01", "high_date", "2026-11-02"),
        ("2026-12-01", "close_vs_floor", 0.0),
        ("2026-12-01", "floor_date", "2026-03-31"),
        ("2026-12-01", "close_vs_line", 0.0),
        ("2026-12-01", "sgov_tr", 0.0),
        ("2027-02-12", "close_vs_x", 0.01),
        ("2027-01-12", "rule_actions", [{"kind": "fire", "reason": "L"}]),
    ],
)
def test_check_day_rereads_the_price_file(
    written: tuple[Path, Path], day: str, key: str, value: Any
) -> None:
    daily, ledger = written
    assert ff.check_day(ledger, daily, date.fromisoformat(day)) == []
    _edit(ledger, _seq(ledger, day), _set(key, value))
    assert ff.verify(ledger) == []
    assert ff.check_day(ledger, daily, date.fromisoformat(day)) != []


def test_check_day_catches_a_changed_rule_frac(
    appended: Callable[[str], Path], tmp_path: Path, synthetic: None
) -> None:
    daily, ledger = _copy({"ladder": appended("ladder")}, "ladder", tmp_path)
    day = "2027-01-05"  # out with the 0.9 tranche, no decision that day
    assert ff.check_day(ledger, daily, date.fromisoformat(day)) == []
    _edit(ledger, _seq(ledger, day), _shift("rule_frac", 1e-6))
    assert ff.verify(ledger) == []
    assert ff.check_day(ledger, daily, date.fromisoformat(day)) != []


def test_check_day_on_the_first_row_resimulates_the_start(written: tuple[Path, Path]) -> None:
    daily, ledger = written
    assert ff.check_day(ledger, daily, ff.FROZEN_START) == []
    assert ff.check_day(ledger, daily, date(2026, 11, 20)) == []  # before the start: nothing


# weekly ------------------------------------------------------------------------------------------

FOOTER = "\nproduction_effect=none；broker_action=none。\n"


def test_weekly_keeps_every_earlier_section_and_adds_the_frozen_one(
    written: tuple[Path, Path],
) -> None:
    daily, ledger = written
    fl.write_weekly(ledger, None)
    fa.extend_weekly(ledger)
    original = (ledger / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    ff.extend_weekly(ledger)
    text = (ledger / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    assert text.startswith(original[: -len(FOOTER)]) and text.endswith(FOOTER)
    section = text[len(original) - len(FOOTER) :]
    assert f"{ff.SECTION}（自 {ff.FROZEN_START}" in section
    with pytest.raises(ff.FrozenLinesError):
        ff.extend_weekly(ledger)


def test_the_weekly_section_prints_the_numbers(written: tuple[Path, Path]) -> None:
    _, ledger = written
    main = fl.read_ledger(ledger)
    rows = ff.read_frozen(ledger)
    text = "\n".join(ff.render_section(main, rows))
    by_seq = {m["seq"]: m for m in main}
    days = len(rows)

    def line(label: str, returns: list[float], exposures: list[float]) -> str:
        wealth, peak, drawdown = 1.0, 1.0, 0.0
        for value in returns:
            wealth *= 1 + value
            peak = max(peak, wealth)
            drawdown = min(drawdown, wealth / peak - 1)
        return (
            f"| {label} | {days} | 0 | {(wealth - 1) * 100:+.2f}% | {drawdown * 100:+.2f}% | "
            f"{sum(exposures) / len(exposures):.2f} |"
        )

    matched = [by_seq[r["ledger_seq"]] for r in rows]
    assert (
        line(
            "冻结的线（账本口径）", [r["ret_frozen"] for r in rows], [r["exp_frozen"] for r in rows]
        )
        in text
    )
    assert line("买入持有", [m["qqq_tr"] for m in matched], [1.0] * days) in text
    assert line("F0001（主账本）", [m["ret_overlay"] for m in matched], [0.5] * days) in text
    assert line("固定 0.8（主账本）", [m["ret_fixed"] for m in matched], [0.8] * days) in text
    idle = [r["ret_frozen"] + (1 - r["exp_frozen"]) * r["sgov_tr"] for r in rows]
    assert f"冻结的线累计 {(math.prod(1 + v for v in idle) - 1) * 100:+.2f}%" in text
    weights = [r["exp_frozen"] for r in rows]
    turnover = sum(abs(b - a) for a, b in zip(weights, weights[1:], strict=False))
    trades = sum(r["trade_frozen"] is not None for r in rows)
    assert f"账本调仓 {trades} 次，换手 {turnover:.2f}" in text
    assert f"{turnover * 0.0005 * 100:.3f} 点" in text
    for day in ("2027-01-12", "2027-01-13", "2027-02-04", "2027-02-05", "2027-02-11", "2027-03-01"):
        assert f"- {day}：" in text, day
    assert "−43.84%（2000-03-24 → 2001-04-04" in text and "−46.08%" in text
    assert "−37.21%" in text and "−35.12%" in text
    assert "冻结线落后主账本 0 行。" in text
    last = rows[-1]
    assert f"X2 最高 {last['high_date']}" in text and f"L 止损 {last['floor_date']}" in text


def test_the_weekly_section_puts_only_the_idle_part_in_sgov(
    appended: Callable[[str], Path],
) -> None:
    ledger = appended("ladder") / "forward"
    rows = ff.read_frozen(ledger)
    assert any(0.0 < r["exp_frozen"] < 1.0 for r in rows)  # holding tranches
    text = "\n".join(ff.render_section(fl.read_ledger(ledger), rows))
    idle = [r["ret_frozen"] + (1 - r["exp_frozen"]) * r["sgov_tr"] for r in rows]
    assert f"冻结的线累计 {(math.prod(1 + v for v in idle) - 1) * 100:+.2f}%" in text


def test_the_weekly_section_before_any_row(recovery: tuple[Path, Path]) -> None:
    _, ledger = recovery
    _truncate_main(ledger, date(2026, 11, 20))
    text = "\n".join(ff.render_section(fl.read_ledger(ledger), []))
    assert "冻结的线自 2026-11-23 起记，还没有行。" in text
    assert "冻结线落后主账本 0 行。" in text and "−43.84%" in text


# The three commands and the guard command ---------------------------------------------------------


def _cli(*args: str) -> Any:
    return CliRunner().invoke(fl.app, list(args))


@pytest.fixture()
def cli_ready(
    recovery: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[Path, Path]]:
    monkeypatch.setattr(fl, "git_provenance", lambda anchor: dict(PROVENANCE))
    monkeypatch.setattr(fa, "ADDED_START", date(2027, 6, 1))  # the added lines stay empty here
    daily, ledger = recovery
    _truncate_main(ledger, date(2026, 12, 11))
    yield daily, ledger


def _append_cli(daily: Path, ledger: Path, through: str) -> Any:
    return _cli(
        "append", "--through", through, "--daily-root", str(daily), "--ledger-dir", str(ledger)
    )


def test_append_writes_the_main_ledger_then_the_added_lines_then_the_frozen_line(
    cli_ready: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    daily, ledger = cli_ready
    monkeypatch.setattr(fl, "append", lambda *a, **k: [])
    order: list[str] = []
    real_added, real_frozen = fa.after_append, ff.after_append

    def added(*args: Any) -> bool:
        order.append("added")
        return real_added(*args)

    def frozen(*args: Any) -> bool:
        order.append("frozen")
        return real_frozen(*args)

    monkeypatch.setattr(fa, "after_append", added)
    monkeypatch.setattr(ff, "after_append", frozen)
    result = _append_cli(daily, ledger, "2026-12-11")
    assert result.exit_code == 0, result.output
    assert order == ["added", "frozen"]
    count = len(fl.trading_days(ff.FROZEN_START, date(2026, 12, 11)))
    assert f"forward_frozen: 追加 {count} 行，共 {count} 行" in result.output


def test_either_failure_exits_one_and_never_stops_the_other(
    cli_ready: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    daily, ledger = cli_ready
    monkeypatch.setattr(fl, "append", lambda *a, **k: [])
    main_before = (ledger / fl.LEDGER_FILE).read_bytes()
    monkeypatch.setattr(fa, "after_append", lambda *a: False)
    result = _append_cli(daily, ledger, "2026-12-11")
    assert result.exit_code == 1
    assert len(_rows(ledger)) == len(fl.trading_days(ff.FROZEN_START, date(2026, 12, 11)))
    monkeypatch.setattr(fa, "after_append", lambda *a: True)
    (ledger / ff.FROZEN_FILE).unlink()
    monkeypatch.setattr(ff, "PRINTED", ())  # check A fails: the frozen line writes nothing
    result = _append_cli(daily, ledger, "2026-12-11")
    assert result.exit_code == 1
    assert "forward_frozen:" in result.output
    assert not (ledger / ff.FROZEN_FILE).exists()
    assert (ledger / fl.LEDGER_FILE).read_bytes() == main_before


def test_a_frozen_failure_keeps_the_real_main_ledger_and_added_lines(
    built: dict[str, Path], tmp_path: Path, synthetic: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    monkeypatch.setattr(ff, "PRINTED", ())
    ok = ff.after_append(ledger, daily, PROVENANCE, lambda line: None)
    assert ok is False and not (ledger / ff.FROZEN_FILE).exists()
    assert fl.verify(ledger) == [] and fa.verify(ledger) == []


def test_check_command_reports_the_frozen_line_after_the_added_lines(
    cli_ready: tuple[Path, Path],
) -> None:
    daily, ledger = cli_ready
    _append(daily, ledger)
    count = len(fl.trading_days(ff.FROZEN_START, date(2026, 12, 11)))
    result = _cli("check", "--ledger-dir", str(ledger), "--daily-root", str(daily))
    assert result.exit_code == 0, result.output
    assert result.output.index("forward_added:") < result.output.index("forward_frozen:")
    assert f"forward_frozen: 冻结的线完好，{count} 行（2026-11-23..2026-12-11）" in result.output
    lines: list[str] = []
    assert ff.after_check(ledger, date(2026, 12, 1), daily, lines.append) is True
    _edit(ledger, 4, _shift("close_vs_line", 0.01))
    assert ff.after_check(ledger, date(2026, 11, 27), daily, lines.append) is False
    result = _cli("check", "--ledger-dir", str(ledger))
    assert result.exit_code == 0, result.output  # without prices only the file is checked
    _edit(ledger, 4, _shift("ret_frozen", 0.01))
    result = _cli("check", "--ledger-dir", str(ledger))
    assert result.exit_code == 1 and "forward_frozen:" in result.output


def test_weekly_command_adds_both_sections(cli_ready: tuple[Path, Path]) -> None:
    daily, ledger = cli_ready
    _append(daily, ledger)
    result = _cli("weekly", "--ledger-dir", str(ledger))
    assert result.exit_code == 0, result.output
    text = (ledger / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    assert text.index(fa.SECTION) < text.index(ff.SECTION)
    assert "forward_frozen: 周汇总加了「冻结的线」一节" in result.output
    _edit(ledger, 3, _shift("ret_frozen", 0.01))
    result = _cli("weekly", "--ledger-dir", str(ledger))
    assert result.exit_code == 1
    text = (ledger / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    assert fa.SECTION in text and ff.SECTION not in text


def test_the_guard_command_only_reads(
    built: dict[str, Path], tmp_path: Path, synthetic: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    daily, ledger = _copy(built, "recovery", tmp_path)
    before = sorted(p.name for p in ledger.iterdir())
    runner = CliRunner()
    args = ["guard", "--daily-root", str(daily), "--ledger-dir", str(ledger)]
    result = runner.invoke(ff.app, args)
    assert result.exit_code == 0, result.output
    assert "核对 A 过" in result.output
    assert "2026-12-16 X2" in result.output and "2027-02-25" in result.output
    result = runner.invoke(ff.app, [*args, "--price-file", fl.PRICES_REL.format(day="2026-12-01")])
    assert result.exit_code == 0, result.output
    assert "2026-12-16" not in result.output
    monkeypatch.setattr(ff, "PRINTED", ())
    result = runner.invoke(ff.app, args)
    assert result.exit_code == 1 and "核对 A 不过" in result.output
    assert sorted(p.name for p in ledger.iterdir()) == before
