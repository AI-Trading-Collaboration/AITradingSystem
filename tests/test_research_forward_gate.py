"""Acceptance tests for TRADING-2565-142: the forward ledger's gate-hint line (闸门线（提示）).

Requirement: knowledge qqq-options/auto-loop/period8/gate-line-requirements-2026-11-27.md
sections 2-4 (period 8 charter 3.4 / 5.1, owner_decision:TRADING-2565:2026-11-27:research_loop_p8).
The line holds the midpoint of the snapshot's ``gated_risk_asset_ai_band`` at t+2 from
2026-11-24, in its own file, and never blocks the main ledger. Synthetic files only.
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
from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.research_probes import forward_added as fa
from ai_trading_system.research_probes import forward_frozen as ff
from ai_trading_system.research_probes import forward_gate as fg
from ai_trading_system.research_probes import forward_ledger as fl
from ai_trading_system.trading_calendar import is_us_equity_trading_day

MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "forward_gate.py"
LEDGER_MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "forward_ledger.py"
README = PROJECT_ROOT / "research" / "_forward" / "README.md"
RECORDS = PROJECT_ROOT / "research" / "_loop" / "forward_records.md"
SYSTEM_FLOW = PROJECT_ROOT / "docs" / "system_flow.md"
REQUIREMENTS = PROJECT_ROOT / "docs" / "requirements" / "TRADING-2565_Research_Bench.md"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
PROVENANCE = {"commit": COMMIT, "branch": "main", "code_modified": False}
DECISION = "owner_decision:TRADING-2565:2026-11-27:research_loop_p8"
FOOTER = "\nproduction_effect=none；broker_action=none。\n"

PRICE_FROM = date(2026, 9, 1)
END = date(2027, 1, 8)
SWITCH = date(2026, 11, 20)  # first full-exposure snapshot (as_of)
# Snapshot days (as_of) with something special about them.
NO_SNAPSHOT = date(2026, 11, 23)
ZERO = date(2026, 12, 1)
BAD_BAND = date(2026, 12, 3)
NO_KEY = date(2026, 12, 7)
NO_TOTAL = date(2026, 12, 9)
NOT_JSON = date(2026, 12, 11)
NO_LABEL = date(2026, 12, 14)
GATED_MIN = date(2026, 12, 16)
# Ledger row days with something special about them.
NO_QQQ = date(2026, 12, 21)
NO_SGOV = date(2026, 12, 22)
GATE_PATTERN = ((0.2, 0.4), (0.4, 0.4), (0.4, 0.6), (0.2, 0.3), (0.6, 0.7))
BUDGET = (0.35, 0.55)


def _qqq_return(day: date) -> float:
    return 0.0004 + (((day.toordinal() * 37) % 29) - 14) / 1000


def _sgov_return(day: date) -> float:
    return 0.00015 + (day.toordinal() % 3) / 100000


def _series(returns: Any) -> pd.Series:
    days = fl.trading_days(PRICE_FROM, END)
    values = 100.0 * np.cumprod([1.0] + [1.0 + returns(d) for d in days[1:]])
    return pd.Series(values, index=pd.DatetimeIndex(days))


QQQ = _series(_qqq_return)
SGOV = _series(_sgov_return)


def _gate(day: date) -> tuple[float, float]:
    if day == ZERO:
        return 0.0, 0.0
    return GATE_PATTERN[len(fl.trading_days(SWITCH, day)) % len(GATE_PATTERN)]


def _gate_total(day: date) -> tuple[float, float]:
    low, high = _gate(day)
    return low * BUDGET[0], high * BUDGET[1]


def _final_old(day: date) -> tuple[float, float]:
    k = day.toordinal() % 3
    return 0.2 + 0.1 * k, 0.4 + 0.1 * k


def _band(low: float, high: float, label: str | None) -> dict[str, Any]:
    band: dict[str, Any] = {"min_position": low, "max_position": high}
    if label is not None:
        band["label"] = label
    return band


def snapshot_payload(day: date) -> dict[str, Any] | str:
    if day == NOT_JSON:
        return "{not json"
    model = _band(0.4, 0.6, "中性")
    if day < SWITCH:
        low, high = _final_old(day)
        return {
            "positions": {
                fl.BAND_KEY: _band(low, high, "中性"),
                fl.MODEL_BAND_KEY: model,
                "total_asset_ai_band": _band(low * 0.6, high * 0.8, "中性"),
            }
        }
    gate, total = _gate(day), _gate_total(day)
    label = None if day == NO_LABEL else "防守"
    full = _band(1.0, 1.0, "满仓（默认）")
    positions: dict[str, Any] = {
        fl.BAND_KEY: full,
        fl.MODEL_BAND_KEY: model,
        "total_asset_ai_band": full,
        fg.GATE_KEY: _band(*gate, label),
        fg.GATE_TOTAL_KEY: _band(*total, label),
        "position_policy": {"mode": "full_exposure_default", "owner_decision": fl.POLICY_DECISION},
    }
    if day == BAD_BAND:
        positions[fg.GATE_KEY] = _band(0.6, 0.4, "防守")
    if day == NO_KEY:
        del positions[fg.GATE_KEY]
    if day == NO_TOTAL:
        del positions[fg.GATE_TOTAL_KEY]
    if day == GATED_MIN:
        positions[fl.BAND_KEY] = _band(*gate, "防守")
        positions["total_asset_ai_band"] = _band(*total, "防守")
        positions["position_policy"] = {"mode": "gated_min", "owner_decision": None}
    return {"positions": positions}


def _write_snapshot(root: Path, day: date, payload: dict[str, Any] | str) -> None:
    path = root / fl.SNAPSHOT_REL.format(day=day.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    path.write_text(text, encoding="utf-8")


def _write_capture(root: Path, day: date) -> None:
    path = root / fl.PRICES_REL.format(day=day.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for stamp in QQQ.index[QQQ.index <= pd.Timestamp(day)]:
        text = stamp.date().isoformat()
        rows.append(f"{text},QQQ,1,1,1,1,{QQQ[stamp]:.10f},100")
        if stamp.date() != NO_SGOV:
            rows.append(f"{text},SGOV,1,1,1,1,{SGOV[stamp]:.10f},100")
    path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )


@pytest.fixture()
def daily_root(tmp_path: Path) -> Path:
    root = tmp_path / "daily"
    for day in fl.trading_days(fl.HELD_FROM, END):
        _write_capture(root, day)
        if day != NO_SNAPSHOT:
            _write_snapshot(root, day, snapshot_payload(day))
    return root


@pytest.fixture()
def ledger_dir(tmp_path: Path) -> Path:
    path = tmp_path / "forward"
    path.mkdir()
    return path


@pytest.fixture(autouse=True)
def _other_lines_after_these_files(monkeypatch: pytest.MonkeyPatch) -> None:
    """The added and frozen lines start after these synthetic files end: no rows here."""
    monkeypatch.setattr(fa, "ADDED_START", date(2027, 6, 1))
    monkeypatch.setattr(ff, "FROZEN_START", date(2027, 6, 1))


def _read_band_or_none(root: Path, day: date) -> fl.Band | None:
    try:
        return fl.read_band(root, day)
    except fl.ForwardLedgerError:
        return None


def _main_through(ledger_dir: Path, root: Path, through: date) -> list[dict[str, Any]]:
    """Append valid main-ledger rows up to through (the main ledger's own rules, simplified)."""
    existing = fl.read_ledger(ledger_dir)
    raw = fl._ledger_lines(ledger_dir)
    prev = chain_digest(raw[-1]) if raw else None
    start = (
        fl.BACKFILL_START
        if not existing
        else fl._next_day(date.fromisoformat(existing[-1]["date"]))
    )
    days = fl.trading_days(fl.HELD_FROM, through)
    rows: list[dict[str, Any]] = []
    for day in fl.trading_days(start, through):
        q = None if day == NO_QQQ else _qqq_return(day)
        band = _read_band_or_none(root, day)
        held = None
        for earlier in reversed(days[: max(0, days.index(day) - 1)]):
            found = _read_band_or_none(root, earlier)
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
            "exp_overlay": 0.5,
            "ret_daily": fl._product(held, q),
            "ret_fixed": fl._product(fl.FIXED_EXPOSURE, q),
            "ret_overlay": fl._product(0.5, q),
            "notes": [] if q is not None else ["没有 QQQ 当天的收益"],
            "code_commit": COMMIT,
            "code_modified": False,
            "prev_sha256": prev,
        }
        rows.append(row)
        prev = chain_digest(fl._row_bytes(row))
    with (ledger_dir / fl.LEDGER_FILE).open("ab") as sink:
        sink.write(b"".join(fl._row_bytes(row) + b"\n" for row in rows))
    assert fl.verify(ledger_dir) == []
    return existing + rows


def _gate_bytes(ledger_dir: Path) -> bytes:
    return (ledger_dir / fg.GATE_FILE).read_bytes()


def _rows(ledger_dir: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in _gate_bytes(ledger_dir).splitlines()]


def _dump(row: dict[str, Any]) -> bytes:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _write_rows(ledger_dir: Path, rows: list[dict[str, Any]], *, rechain: bool = True) -> None:
    previous = None
    for k, row in enumerate(rows):
        if rechain:
            row["prev_sha256"] = None if previous is None else chain_digest(previous)
        previous = _dump(row)
        rows[k] = row
    (ledger_dir / fg.GATE_FILE).write_bytes(b"".join(_dump(row) + b"\n" for row in rows))


def _held_day(day: date, root: Path) -> date | None:
    """Independent t+2 lookup with the 2026-11-20 floor."""
    days = fl.trading_days(SWITCH, day)
    for earlier in reversed(days[:-2]):
        if (root / fl.SNAPSHOT_REL.format(day=earlier.isoformat())).is_file():
            return earlier
    return None


def _sgov(root: Path, price_file: str, day: date) -> float | None:
    series = fl.read_capture(root, price_file, fl.CASH_TICKER)
    return None if series is None else fl.day_return(series, day)


def _expected(main: dict[str, Any], root: Path) -> dict[str, Any]:
    """The requirement's columns for one main row, from the synthetic definitions."""
    day = date.fromisoformat(main["date"])
    q = main["qqq_tr"]
    held = _held_day(day, root)
    out: dict[str, Any] = {
        "gate_snapshot_file": None
        if held is None
        else fl.SNAPSHOT_REL.format(day=held.isoformat()),
        "policy_mode": None,
        "gate_min": None,
        "gate_max": None,
        "gate_label": None,
        "exp_gate_held": None,
        "ret_gate": None,
        "gate_total_min": None,
        "gate_total_max": None,
        "exp_gate_total_held": None,
        "ret_gate_total": None,
        "sgov_tr": _sgov(root, main["price_file"], day),
    }
    if held is None or held == NOT_JSON:
        return out
    out["policy_mode"] = "gated_min" if held == GATED_MIN else "full_exposure_default"
    if held not in (BAD_BAND, NO_KEY):
        low, high = _gate(held)
        mid = (low + high) / 2.0
        out.update(
            gate_min=low,
            gate_max=high,
            gate_label=None if held == NO_LABEL else "防守",
            exp_gate_held=mid,
            ret_gate=None if q is None else mid * q,
        )
    if held != NO_TOTAL:
        low, high = _gate_total(held)
        mid = (low + high) / 2.0
        out.update(
            gate_total_min=low,
            gate_total_max=high,
            exp_gate_total_held=mid,
            ret_gate_total=None if q is None else mid * q,
        )
    return out


def _append(ledger_dir: Path, root: Path) -> list[dict[str, Any]]:
    return fg.append(ledger_dir, root, provenance=PROVENANCE)


def _row_on(rows: list[dict[str, Any]], held: date) -> dict[str, Any]:
    target = fl.SNAPSHOT_REL.format(day=held.isoformat())
    return next(row for row in rows if row["gate_snapshot_file"] == target)


# Constants and boundaries ------------------------------------------------------------------------


def test_constants_name_the_run_the_decision_the_file_and_the_start() -> None:
    assert fg.RUN_ID == "TRADING-2565-142"
    assert fg.OWNER_DECISION == DECISION
    assert fg.SCHEMA == "forward_gate.v1"
    assert fg.GATE_FILE == "gate_lines.jsonl"
    assert fg.GATE_START == date(2026, 11, 24)
    assert fg.GATE_HELD_FROM == date(2026, 11, 20)
    assert is_us_equity_trading_day(fg.GATE_START) and is_us_equity_trading_day(fg.GATE_HELD_FROM)
    assert fl.trading_days(fg.GATE_HELD_FROM, fg.GATE_START)[-3] == fg.GATE_HELD_FROM
    assert fg.GATE_KEY == "gated_risk_asset_ai_band"
    assert fg.GATE_TOTAL_KEY == "gated_total_asset_ai_band"
    assert fg.SECTION == "## 闸门线（提示）"
    assert fg.ROW_KEYS == {
        "schema_version",
        "seq",
        "date",
        "ledger_seq",
        "ledger_sha256",
        "qqq_tr",
        "price_file",
        "gate_snapshot_file",
        "policy_mode",
        "gate_min",
        "gate_max",
        "gate_label",
        "exp_gate_held",
        "ret_gate",
        "gate_total_min",
        "gate_total_max",
        "exp_gate_total_held",
        "ret_gate_total",
        "sgov_tr",
        "notes",
        "code_commit",
        "code_modified",
        "prev_sha256",
    }


def test_committed_gate_lines_pass_check() -> None:
    # Passes before the first Friday append too, when there is no gate file yet.
    assert fg.verify(fl.DEFAULT_LEDGER_DIR) == []


def test_committed_weekly_has_the_gate_section_once_the_file_has_rows() -> None:
    if not (fl.DEFAULT_LEDGER_DIR / fg.GATE_FILE).exists():
        return
    rows = fg.read_gate(fl.DEFAULT_LEDGER_DIR)
    text = (fl.DEFAULT_LEDGER_DIR / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    assert text.count(fg.SECTION) == 1
    assert text.index("## 冻结的线") < text.index(fg.SECTION)
    assert rows and rows[0]["date"] == fg.GATE_START.isoformat()
    assert f"| 闸门线（提示） | {len(rows)} |" in text


def test_the_hooks_follow_the_frozen_line_in_three_commands_and_import_lazily() -> None:
    tree = ast.parse(LEDGER_MODULE.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ImportFrom | ast.Import):
            assert "forward_gate" not in ast.dump(node)
    calls: dict[str, list[tuple[str, str]]] = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            for inner in ast.walk(node):
                if (
                    isinstance(inner, ast.Attribute)
                    and isinstance(inner.value, ast.Name)
                    and inner.value.id in ("forward_frozen", "forward_gate")
                ):
                    calls.setdefault(node.name, []).append((inner.value.id, inner.attr))
    assert calls == {
        "append_command": [("forward_frozen", "after_append"), ("forward_gate", "after_append")],
        "weekly_command": [("forward_frozen", "after_weekly"), ("forward_gate", "after_weekly")],
        "check_command": [("forward_frozen", "after_check"), ("forward_gate", "after_check")],
    }


def test_the_module_never_writes_the_main_ledger_and_reads_nothing_else() -> None:
    source = MODULE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign | ast.AugAssign):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                assert not (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id in ("fl", "fa", "ff")
                )
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name) and node.func.value.id == "fl":
                assert node.func.attr not in {"append", "build_rows", "write_weekly"}
    for words in ("requests", "urllib", "httpx", "us_market", "SPY", "fmp_"):
        assert words not in source, words


def test_the_docs_name_the_line_its_file_and_its_record() -> None:
    readme = README.read_text(encoding="utf-8")
    assert "## 闸门线（提示）" in readme and "gate_lines.jsonl" in readme
    assert "forward_gate.v1" in readme and "2026-11-24" in readme and "2026-11-20" in readme
    assert "tests/test_research_forward_gate.py" in readme
    records = RECORDS.read_text(encoding="utf-8")
    assert "| F0011 | TRADING-2565-142 发布那天 |" in records
    assert records.index("| F0010 |") < records.index("| F0011 |")
    assert DECISION in records
    flow = SYSTEM_FLOW.read_text(encoding="utf-8")
    assert "forward_gate" in flow and "gated_risk_asset_ai_band" in flow
    assert "TRADING-2565-142" in REQUIREMENTS.read_text(encoding="utf-8")


# The held snapshot -------------------------------------------------------------------------------


def test_the_held_snapshot_is_two_trading_days_back_with_fallback_and_a_floor(
    daily_root: Path,
) -> None:
    def name(day: date) -> str:
        return fl.SNAPSHOT_REL.format(day=day.isoformat())

    assert fg.held_snapshot(daily_root, date(2026, 11, 24)) == name(date(2026, 11, 20))
    # 11-23 has no snapshot: 11-25 falls back to 11-20.
    assert fg.held_snapshot(daily_root, date(2026, 11, 25)) == name(date(2026, 11, 20))
    assert fg.held_snapshot(daily_root, date(2026, 11, 27)) == name(date(2026, 11, 24))
    assert fg.held_snapshot(daily_root, date(2026, 12, 3)) == name(date(2026, 12, 1))
    # An unreadable file still is the held file.
    assert fg.held_snapshot(daily_root, date(2026, 12, 15)) == name(NOT_JSON)
    (daily_root / name(SWITCH)).unlink()
    assert (daily_root / name(date(2026, 11, 19))).is_file()
    assert fg.held_snapshot(daily_root, date(2026, 11, 24)) is None
    assert fg.held_snapshot(daily_root, date(2026, 11, 25)) is None


# Appending ---------------------------------------------------------------------------------------


def test_rows_start_at_the_start_and_hang_on_the_main_rows(
    daily_root: Path, ledger_dir: Path
) -> None:
    main = _main_through(ledger_dir, daily_root, END)
    rows = _append(ledger_dir, daily_root)
    assert rows == _rows(ledger_dir)
    raw = fl._ledger_lines(ledger_dir)
    after = [(m, line) for m, line in zip(main, raw, strict=True) if m["date"] >= "2026-11-24"]
    assert len(rows) == len(after)
    previous = None
    for k, (row, (m, line)) in enumerate(zip(rows, after, strict=True), 1):
        assert row.keys() == fg.ROW_KEYS
        assert row["schema_version"] == fg.SCHEMA and row["seq"] == k
        assert (row["date"], row["ledger_seq"]) == (m["date"], m["seq"])
        assert row["ledger_sha256"] == chain_digest(line)
        assert (row["qqq_tr"], row["price_file"]) == (m["qqq_tr"], m["price_file"])
        assert row["prev_sha256"] == (None if previous is None else chain_digest(previous))
        assert (row["code_commit"], row["code_modified"]) == (COMMIT, False)
        assert isinstance(row["notes"], list)
        previous = _dump(row)
    assert rows[0]["date"] == "2026-11-24" and rows[-1]["date"] == END.isoformat()
    assert fg.verify(ledger_dir) == []


def test_every_column_follows_the_written_rule(daily_root: Path, ledger_dir: Path) -> None:
    main = _main_through(ledger_dir, daily_root, END)
    rows = _append(ledger_dir, daily_root)
    by_seq = {m["seq"]: m for m in main}
    for row in rows:
        expected = _expected(by_seq[row["ledger_seq"]], daily_root)
        for key, value in expected.items():
            if isinstance(value, float):
                assert row[key] == pytest.approx(value, abs=1e-12), (row["date"], key)
            else:
                assert row[key] == value, (row["date"], key)
        if row["gate_min"] is None:
            assert row["notes"], row["date"]


def test_the_first_two_rows_hold_the_first_full_exposure_snapshot(
    daily_root: Path, ledger_dir: Path
) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 11, 25))
    first, second = _append(ledger_dir, daily_root)
    for row in (first, second):
        assert row["gate_snapshot_file"] == fl.SNAPSHOT_REL.format(day="2026-11-20")
        assert (row["gate_min"], row["gate_max"], row["gate_label"]) == (0.4, 0.4, "防守")
        assert row["policy_mode"] == "full_exposure_default"
    assert (first["date"], second["date"]) == ("2026-11-24", "2026-11-25")


def test_a_zero_band_is_exposure_zero_not_missing(daily_root: Path, ledger_dir: Path) -> None:
    _main_through(ledger_dir, daily_root, END)
    row = _row_on(_append(ledger_dir, daily_root), ZERO)
    assert (row["gate_min"], row["gate_max"], row["exp_gate_held"]) == (0.0, 0.0, 0.0)
    assert row["ret_gate"] == 0.0
    assert (row["exp_gate_total_held"], row["ret_gate_total"]) == (0.0, 0.0)


@pytest.mark.parametrize("held", [BAD_BAND, NO_KEY])
def test_a_missing_or_invalid_gate_band_blanks_only_its_columns(
    daily_root: Path, ledger_dir: Path, held: date
) -> None:
    _main_through(ledger_dir, daily_root, END)
    row = _row_on(_append(ledger_dir, daily_root), held)
    assert all(row[key] is None for key in ("gate_min", "gate_max", "gate_label", "exp_gate_held"))
    assert row["ret_gate"] is None and row["notes"]
    assert row["exp_gate_total_held"] == pytest.approx(sum(_gate_total(held)) / 2.0)
    assert row["policy_mode"] == "full_exposure_default"


@pytest.mark.parametrize(
    "bounds", [(0.4, 1.2), (-0.1, 0.4), ("0.2", 0.4), (0.4, None), (math.nan, 0.4), (True, 1)]
)
def test_a_gate_band_outside_zero_to_one_is_invalid(
    daily_root: Path, ledger_dir: Path, bounds: tuple[Any, Any]
) -> None:
    payload = snapshot_payload(BAD_BAND)
    assert isinstance(payload, dict)
    payload["positions"][fg.GATE_KEY] = {
        "min_position": bounds[0],
        "max_position": bounds[1],
        "label": "防守",
    }
    _write_snapshot(daily_root, BAD_BAND, payload)
    _main_through(ledger_dir, daily_root, END)
    row = _row_on(_append(ledger_dir, daily_root), BAD_BAND)
    assert row["gate_min"] is None and row["gate_max"] is None and row["gate_label"] is None
    assert row["exp_gate_held"] is None and row["notes"]
    assert fg.verify(ledger_dir) == []


def test_an_unreadable_snapshot_keeps_its_name_and_blanks_the_rest(
    daily_root: Path, ledger_dir: Path
) -> None:
    _main_through(ledger_dir, daily_root, END)
    row = _row_on(_append(ledger_dir, daily_root), NOT_JSON)
    blank = ("policy_mode", "gate_min", "gate_max", "exp_gate_held", "ret_gate")
    assert all(row[key] is None for key in blank)
    assert row["exp_gate_total_held"] is None and row["notes"]


def test_a_missing_total_blanks_only_the_total_columns(daily_root: Path, ledger_dir: Path) -> None:
    _main_through(ledger_dir, daily_root, END)
    row = _row_on(_append(ledger_dir, daily_root), NO_TOTAL)
    assert row["exp_gate_held"] == pytest.approx(sum(_gate(NO_TOTAL)) / 2.0)
    total = ("gate_total_min", "gate_total_max", "exp_gate_total_held", "ret_gate_total")
    assert all(row[key] is None for key in total)
    assert row["notes"]


def test_label_and_policy_mode_are_copied_or_blank(daily_root: Path, ledger_dir: Path) -> None:
    _main_through(ledger_dir, daily_root, END)
    rows = _append(ledger_dir, daily_root)
    assert _row_on(rows, NO_LABEL)["gate_label"] is None
    assert _row_on(rows, NO_LABEL)["exp_gate_held"] is not None
    gated_min = _row_on(rows, GATED_MIN)
    assert gated_min["policy_mode"] == "gated_min"
    assert gated_min["exp_gate_held"] == pytest.approx(sum(_gate(GATED_MIN)) / 2.0)


def test_a_missing_qqq_return_keeps_the_exposure_and_blanks_the_returns(
    daily_root: Path, ledger_dir: Path
) -> None:
    _main_through(ledger_dir, daily_root, END)
    row = next(r for r in _append(ledger_dir, daily_root) if r["date"] == NO_QQQ.isoformat())
    assert row["qqq_tr"] is None and row["exp_gate_held"] is not None
    assert row["ret_gate"] is None and row["ret_gate_total"] is None


def test_sgov_comes_from_the_rows_price_file(daily_root: Path, ledger_dir: Path) -> None:
    _main_through(ledger_dir, daily_root, END)
    rows = {row["date"]: row for row in _append(ledger_dir, daily_root)}
    assert rows[NO_SGOV.isoformat()]["sgov_tr"] is None
    day = date(2026, 12, 2)
    assert rows[day.isoformat()]["sgov_tr"] == pytest.approx(_sgov_return(day), rel=1e-6)


def test_no_snapshot_from_the_floor_on_gives_an_empty_noted_row(
    daily_root: Path, ledger_dir: Path
) -> None:
    (daily_root / fl.SNAPSHOT_REL.format(day=SWITCH.isoformat())).unlink()
    _main_through(ledger_dir, daily_root, date(2026, 11, 27))
    rows = _append(ledger_dir, daily_root)
    for row in rows[:2]:
        assert row["gate_snapshot_file"] is None and row["policy_mode"] is None
        assert row["exp_gate_held"] is None and row["exp_gate_total_held"] is None
        assert row["notes"]
    assert rows[2]["gate_snapshot_file"] == fl.SNAPSHOT_REL.format(day="2026-11-24")
    assert fg.verify(ledger_dir) == []


def test_before_the_first_row_nothing_is_written(daily_root: Path, ledger_dir: Path) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 11, 23))
    lines: list[str] = []
    assert fg.after_append(ledger_dir, daily_root, PROVENANCE, lines.append) is True
    assert not (ledger_dir / fg.GATE_FILE).exists()
    assert lines == ["forward_gate: 闸门线自 2026-11-24 起记，还没有行"]
    assert fg.verify(ledger_dir) == []


def test_one_append_writes_the_same_bytes_as_several(daily_root: Path, tmp_path: Path) -> None:
    once, steps = tmp_path / "once", tmp_path / "steps"
    once.mkdir()
    steps.mkdir()
    _main_through(once, daily_root, END)
    _append(once, daily_root)
    for through in (date(2026, 11, 25), date(2026, 12, 4), date(2026, 12, 18), END):
        _main_through(steps, daily_root, through)
        _append(steps, daily_root)
    assert _gate_bytes(once) == _gate_bytes(steps)


def test_the_gate_file_may_lag_and_the_next_append_catches_up(
    daily_root: Path, ledger_dir: Path
) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 12, 4))
    _append(ledger_dir, daily_root)
    first = _gate_bytes(ledger_dir)
    _main_through(ledger_dir, daily_root, date(2026, 12, 18))
    assert fg.verify(ledger_dir) == []  # lagging is allowed
    added = _append(ledger_dir, daily_root)
    assert added[0]["date"] == "2026-12-07" and added[-1]["date"] == "2026-12-18"
    assert _gate_bytes(ledger_dir).startswith(first)


def test_a_failure_leaves_the_main_ledger_written_and_the_next_append_fills_in(
    daily_root: Path, ledger_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 11, 25))
    calls = {"n": 0}
    real = fg.held_snapshot

    def flaky(root: Path, day: date) -> str | None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise fg.GateLinesError("synthetic failure")
        return real(root, day)

    monkeypatch.setattr(fg, "held_snapshot", flaky)
    lines: list[str] = []
    assert fg.after_append(ledger_dir, daily_root, PROVENANCE, lines.append) is False
    assert not (ledger_dir / fg.GATE_FILE).exists()  # nothing half-written
    assert lines and lines[-1].startswith("forward_gate: 闸门线没有写（主账本照写）：")
    assert len(fl.read_ledger(ledger_dir)) == 33
    monkeypatch.setattr(fg, "held_snapshot", real)
    assert fg.after_append(ledger_dir, daily_root, PROVENANCE, lines.append) is True
    assert [row["date"] for row in _rows(ledger_dir)] == ["2026-11-24", "2026-11-25"]


def test_an_append_after_a_missing_final_newline_starts_a_new_line(
    daily_root: Path, ledger_dir: Path
) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 12, 4))
    _append(ledger_dir, daily_root)
    path = ledger_dir / fg.GATE_FILE
    path.write_bytes(path.read_bytes().rstrip(b"\n"))
    _main_through(ledger_dir, daily_root, date(2026, 12, 11))
    assert _append(ledger_dir, daily_root)
    assert fg.verify(ledger_dir) == []
    assert path.read_bytes().endswith(b"\n") and b"}{" not in path.read_bytes()


def test_broken_written_rows_block_only_the_gate_file(daily_root: Path, ledger_dir: Path) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 12, 4))
    _append(ledger_dir, daily_root)
    rows = _rows(ledger_dir)
    rows[1]["exp_gate_held"] = 0.9
    _write_rows(ledger_dir, rows)
    before = _gate_bytes(ledger_dir)
    _main_through(ledger_dir, daily_root, date(2026, 12, 11))
    lines: list[str] = []
    assert fg.after_append(ledger_dir, daily_root, PROVENANCE, lines.append) is False
    assert _gate_bytes(ledger_dir) == before
    assert any("line 2" in line for line in lines)


def test_append_refuses_modified_code(daily_root: Path, ledger_dir: Path) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 11, 25))
    with pytest.raises(fg.GateLinesError):
        fg.append(ledger_dir, daily_root, provenance={**PROVENANCE, "code_modified": True})
    with pytest.raises(fg.GateLinesError):
        fg.append(ledger_dir, daily_root, provenance={**PROVENANCE, "commit": "short"})
    assert not (ledger_dir / fg.GATE_FILE).exists()
    # The provenance is checked first, also when there is nothing new to write.
    _append(ledger_dir, daily_root)
    for bad in ({**PROVENANCE, "code_modified": True}, {**PROVENANCE, "commit": "short"}):
        with pytest.raises(fg.GateLinesError):
            fg.append(ledger_dir, daily_root, provenance=bad)


# Commands ----------------------------------------------------------------------------------------


def _fake_main(monkeypatch: pytest.MonkeyPatch) -> None:
    """The main ledger's own append is tested elsewhere; here it writes the simplified rows."""
    monkeypatch.setattr(fl, "git_provenance", lambda _root: PROVENANCE)

    def main_append(
        ledger_dir: Path, daily_root: Path, through: date, *, provenance: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return _main_through(ledger_dir, daily_root, through)

    monkeypatch.setattr(fl, "append", main_append)


def test_the_append_command_prints_the_rows_and_exits_zero(
    daily_root: Path, ledger_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_main(monkeypatch)
    result = CliRunner().invoke(
        fl.app,
        [
            "append",
            "--through",
            "2026-11-25",
            "--daily-root",
            str(daily_root),
            "--ledger-dir",
            str(ledger_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    gate_lines = [line for line in result.output.splitlines() if line.startswith("forward_gate:")]
    assert gate_lines[0].startswith("forward_gate: 2026-11-24 闸门 0.40")
    assert gate_lines[-1] == "forward_gate: 追加 2 行，共 2 行"
    assert result.output.index("forward_frozen:") < result.output.index("forward_gate:")
    assert [row["date"] for row in _rows(ledger_dir)] == ["2026-11-24", "2026-11-25"]


def test_a_gate_failure_alone_makes_the_command_exit_one(
    daily_root: Path, ledger_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_main(monkeypatch)
    _main_through(ledger_dir, daily_root, date(2026, 11, 25))
    _append(ledger_dir, daily_root)
    rows = _rows(ledger_dir)
    rows[0]["ret_gate"] = 1.0
    _write_rows(ledger_dir, rows)
    common = ["--ledger-dir", str(ledger_dir)]
    result = CliRunner().invoke(
        fl.app, ["append", "--through", "2026-12-04", "--daily-root", str(daily_root), *common]
    )
    assert result.exit_code == 1
    assert "forward_ledger: 追加" in result.output  # the main ledger was written
    assert fl.read_ledger(ledger_dir)[-1]["date"] == "2026-12-04"
    checked = CliRunner().invoke(fl.app, ["check", *common])
    assert checked.exit_code == 1
    assert "forward_ledger: 账本完好" in checked.output
    assert "forward_gate: gate_lines.jsonl line 1: " in checked.output


def test_the_check_command_reports_a_sound_gate_file(daily_root: Path, ledger_dir: Path) -> None:
    _main_through(ledger_dir, daily_root, date(2026, 12, 4))
    _append(ledger_dir, daily_root)
    result = CliRunner().invoke(fl.app, ["check", "--ledger-dir", str(ledger_dir)])
    assert result.exit_code == 0, result.output
    assert "forward_gate: 闸门线完好，8 行（2026-11-24..2026-12-04）" in result.output
    day = CliRunner().invoke(
        fl.app,
        [
            "check",
            "--ledger-dir",
            str(ledger_dir),
            "--day",
            "2026-12-03",
            "--daily-root",
            str(daily_root),
        ],
    )
    assert "forward_gate: 闸门线完好" in day.output
    path = daily_root / fl.SNAPSHOT_REL.format(day="2026-12-01")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["positions"][fg.GATE_KEY]["max_position"] = 0.2
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    changed = CliRunner().invoke(
        fl.app,
        [
            "check",
            "--ledger-dir",
            str(ledger_dir),
            "--day",
            "2026-12-03",
            "--daily-root",
            str(daily_root),
        ],
    )
    assert changed.exit_code == 1
    assert "forward_gate: gate_lines.jsonl 2026-12-03: " in changed.output


# check -------------------------------------------------------------------------------------------


@pytest.fixture()
def written(daily_root: Path, ledger_dir: Path) -> list[dict[str, Any]]:
    _main_through(ledger_dir, daily_root, END)
    _append(ledger_dir, daily_root)
    assert fg.verify(ledger_dir) == []
    return _rows(ledger_dir)


def _first(rows: list[dict[str, Any]], **where: Any) -> int:
    for k, row in enumerate(rows):
        if all(row[key] == value for key, value in where.items()):
            return k
    raise AssertionError(where)


def _snapshot_index(rows: list[dict[str, Any]], held: date) -> int:
    return _first(rows, gate_snapshot_file=fl.SNAPSHOT_REL.format(day=held.isoformat()))


BREAKS: dict[str, Any] = {
    "midpoint": lambda rows: (0, {"exp_gate_held": rows[0]["exp_gate_held"] + 0.01}),
    "return": lambda rows: (1, {"ret_gate": rows[1]["ret_gate"] + 1e-9}),
    # The band breaks keep the midpoint and return consistent: only the band rule catches them.
    "band order": lambda rows: (
        0,
        {
            "gate_min": 0.5,
            "gate_max": 0.4,
            "exp_gate_held": 0.45,
            "ret_gate": 0.45 * rows[0]["qqq_tr"],
        },
    ),
    "band above one": lambda rows: (
        0,
        {
            "gate_min": 0.4,
            "gate_max": 1.2,
            "exp_gate_held": 0.8,
            "ret_gate": 0.8 * rows[0]["qqq_tr"],
        },
    ),
    "band as text": lambda rows: (0, {"gate_min": "0.4"}),
    "half a band": lambda rows: (0, {"gate_max": None}),
    "label": lambda rows: (0, {"gate_label": 3}),
    "policy mode": lambda rows: (0, {"policy_mode": 3}),
    "sgov": lambda rows: (0, {"sgov_tr": "0.0001"}),
    "sgov infinite": lambda rows: (0, {"sgov_tr": math.inf}),
    "total midpoint": lambda rows: (2, {"exp_gate_total_held": 0.5}),
    "total return": lambda rows: (2, {"ret_gate_total": 0.5}),
    "notes type": lambda rows: (0, {"notes": "none"}),
    "midpoint without a band": lambda rows: (
        _snapshot_index(rows, BAD_BAND),
        {"exp_gate_held": 0.3},
    ),
    "total midpoint without a band": lambda rows: (
        _snapshot_index(rows, NO_TOTAL),
        {"exp_gate_total_held": 0.1},
    ),
    "commit": lambda rows: (0, {"code_commit": "abc"}),
    "modified": lambda rows: (0, {"code_modified": True}),
    "schema": lambda rows: (0, {"schema_version": "forward_gate.v0"}),
    "snapshot before the floor": lambda rows: (
        0,
        {"gate_snapshot_file": fl.SNAPSHOT_REL.format(day="2026-11-19")},
    ),
    "snapshot too late": lambda rows: (
        0,
        {"gate_snapshot_file": fl.SNAPSHOT_REL.format(day="2026-11-23")},
    ),
    "snapshot name": lambda rows: (0, {"gate_snapshot_file": "decision_snapshot_2026-11-20.json"}),
    "no snapshot but a band": lambda rows: (0, {"gate_snapshot_file": None}),
    "ledger sha": lambda rows: (1, {"ledger_sha256": "0" * 64}),
    "ledger seq": lambda rows: (1, {"ledger_seq": rows[1]["ledger_seq"] + 1}),
    "date": lambda rows: (1, {"date": "2026-11-26"}),
    "qqq": lambda rows: (1, {"qqq_tr": 0.01}),
    "price file": lambda rows: (1, {"price_file": rows[0]["price_file"]}),
    "seq": lambda rows: (1, {"seq": 3}),
}


@pytest.mark.parametrize("name", sorted(BREAKS))
def test_check_catches_each_broken_field(
    name: str, written: list[dict[str, Any]], ledger_dir: Path
) -> None:
    rows = [dict(row) for row in written]
    index, changes = BREAKS[name](rows)
    rows[index].update(changes)
    _write_rows(ledger_dir, rows)
    problems = fg.verify(ledger_dir)
    assert len(problems) == 1, problems
    assert problems[0].startswith(f"{fg.GATE_FILE} line {index + 1}: "), problems


def test_check_needs_a_note_on_every_row_without_a_band(
    written: list[dict[str, Any]], ledger_dir: Path
) -> None:
    rows = [dict(row) for row in written]
    k = _snapshot_index(rows, BAD_BAND)
    rows[k]["notes"] = []
    _write_rows(ledger_dir, rows)
    assert fg.verify(ledger_dir)[0].startswith(f"{fg.GATE_FILE} line {k + 1}: ")


def test_check_catches_a_null_return_with_a_return_day(
    written: list[dict[str, Any]], ledger_dir: Path
) -> None:
    rows = [dict(row) for row in written]
    k = _first(rows, date=NO_QQQ.isoformat())
    rows[k]["ret_gate"] = 0.0
    _write_rows(ledger_dir, rows)
    assert fg.verify(ledger_dir)[0].startswith(f"{fg.GATE_FILE} line {k + 1}: ")
    rows[k]["ret_gate"] = None
    rows[k - 1]["ret_gate"] = None
    _write_rows(ledger_dir, rows)
    assert fg.verify(ledger_dir)[0].startswith(f"{fg.GATE_FILE} line {k}: ")


def test_check_catches_keys_the_chain_and_a_missing_middle_row(
    written: list[dict[str, Any]], ledger_dir: Path
) -> None:
    extra = [dict(row) for row in written]
    extra[0]["extra"] = 1
    _write_rows(ledger_dir, extra)
    assert fg.verify(ledger_dir)[0].startswith(f"{fg.GATE_FILE} line 1: ")
    fewer = [dict(row) for row in written]
    del fewer[2]["notes"]
    _write_rows(ledger_dir, fewer)
    assert fg.verify(ledger_dir)[0].startswith(f"{fg.GATE_FILE} line 3: ")
    unchained = [dict(row) for row in written]
    unchained[3]["notes"] = ["x"]
    _write_rows(ledger_dir, unchained, rechain=False)
    assert fg.verify(ledger_dir)[0].startswith(f"{fg.GATE_FILE} line 5: ")
    gap = [dict(row) for row in written]
    del gap[4]
    for k, row in enumerate(gap, 1):
        row["seq"] = k
    _write_rows(ledger_dir, gap)
    assert fg.verify(ledger_dir)[0].startswith(f"{fg.GATE_FILE} line 5: ")
    (ledger_dir / fg.GATE_FILE).write_bytes(_dump(written[0]) + b"\n{not json\n")
    assert fg.verify(ledger_dir)[0].startswith(f"{fg.GATE_FILE} line 2: ")


def test_the_file_may_end_before_the_main_ledger_but_not_after_it(
    written: list[dict[str, Any]], ledger_dir: Path
) -> None:
    _write_rows(ledger_dir, [dict(row) for row in written[:5]])
    assert fg.verify(ledger_dir) == []
    longer = [dict(row) for row in written]
    extra = dict(longer[-1])
    extra.update(seq=len(longer) + 1, date="2027-01-11", ledger_seq=extra["ledger_seq"] + 1)
    _write_rows(ledger_dir, [*longer, extra])
    assert fg.verify(ledger_dir)[0].startswith(f"{fg.GATE_FILE} line {len(longer) + 1}: ")


def test_check_day_rereads_the_sources(
    written: list[dict[str, Any]], ledger_dir: Path, daily_root: Path
) -> None:
    for day in ("2026-11-24", "2026-11-25", "2026-12-03", "2026-12-09", "2026-12-22"):
        assert fg.check_day(ledger_dir, daily_root, date.fromisoformat(day)) == [], day
    assert fg.check_day(ledger_dir, daily_root, date(2026, 11, 20)) == []  # no row that day


def test_check_day_catches_changed_sources(
    written: list[dict[str, Any]], ledger_dir: Path, daily_root: Path
) -> None:
    day = date(2026, 12, 3)  # holds the 12-01 snapshot (band 0-0)
    path = daily_root / fl.SNAPSHOT_REL.format(day="2026-12-01")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["positions"][fg.GATE_KEY]["max_position"] = 0.2
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    assert fg.check_day(ledger_dir, daily_root, day)
    payload["positions"][fg.GATE_KEY]["max_position"] = 0.0
    payload["positions"]["position_policy"]["mode"] = "gated_min"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    assert fg.check_day(ledger_dir, daily_root, day)
    payload["positions"]["position_policy"]["mode"] = "full_exposure_default"
    payload["positions"][fl.BAND_KEY] = _band(0.2, 0.4, "防守")  # final mid no longer 1.0
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    assert fg.check_day(ledger_dir, daily_root, day)
    payload["positions"][fl.BAND_KEY] = _band(1.0, 1.0, "满仓（默认）")
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    assert fg.check_day(ledger_dir, daily_root, day) == []
    # A newer snapshot for an earlier day changes which file is held, even with the same bands.
    _write_snapshot(daily_root, NO_SNAPSHOT, snapshot_payload(SWITCH))
    assert fg.check_day(ledger_dir, daily_root, date(2026, 11, 25))
    # SGOV in the row's price file changed.
    sgov_day = date(2026, 12, 2)
    prices = daily_root / fl.PRICES_REL.format(day=sgov_day.isoformat())
    text = prices.read_text(encoding="utf-8").replace(
        f"{sgov_day.isoformat()},SGOV,1,1,1,1,", f"{sgov_day.isoformat()},SGOV,1,1,1,1,1"
    )
    prices.write_text(text, encoding="utf-8")
    assert fg.check_day(ledger_dir, daily_root, sgov_day)


# The weekly section ------------------------------------------------------------------------------


def _weekly_before(ledger_dir: Path) -> str:
    fl.write_weekly(ledger_dir)
    fa.extend_weekly(ledger_dir)
    ff.extend_weekly(ledger_dir)
    return (ledger_dir / fl.WEEKLY_FILE).read_text(encoding="utf-8")


def test_the_section_goes_after_the_frozen_line_and_keeps_the_rest(
    written: list[dict[str, Any]], ledger_dir: Path
) -> None:
    before = _weekly_before(ledger_dir)
    fg.extend_weekly(ledger_dir)
    text = (ledger_dir / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    head = before[: -len(FOOTER)]
    assert text.startswith(head) and text.endswith(FOOTER)
    section = text[len(head) : -len(FOOTER)]
    assert section == "\n".join(fg.render_section(fl.read_ledger(ledger_dir), written)) + "\n"
    assert section.startswith(f"\n{fg.SECTION}（自 2026-11-24，只记账，不判）\n")
    assert text.index("## 冻结的线") < text.index(fg.SECTION)
    with pytest.raises(fg.GateLinesError):
        fg.extend_weekly(ledger_dir)


def _main_row(seq: int, day: date, q: float | None, daily: float | None) -> dict[str, Any]:
    return {
        "seq": seq,
        "date": day.isoformat(),
        "qqq_tr": q,
        "exp_daily_held": daily,
        "ret_daily": fl._product(daily, q),
        "exp_fixed": 0.8,
        "ret_fixed": fl._product(0.8, q),
        "exp_overlay": 0.5,
        "ret_overlay": fl._product(0.5, q),
    }


def _gate_row(
    main: dict[str, Any],
    band: tuple[float, float] | None,
    *,
    mode: str | None = "full_exposure_default",
    sgov: float | None = 0.0002,
    label: str = "防守",
) -> dict[str, Any]:
    q = main["qqq_tr"]
    mid = None if band is None else (band[0] + band[1]) / 2.0
    total = None if mid is None else mid * 0.45
    return {
        "date": main["date"],
        "ledger_seq": main["seq"],
        "qqq_tr": q,
        "gate_snapshot_file": None if mode is None else "snap",
        "policy_mode": mode,
        "gate_min": None if band is None else band[0],
        "gate_max": None if band is None else band[1],
        "gate_label": None if band is None else label,
        "exp_gate_held": mid,
        "ret_gate": fl._product(mid, q),
        "exp_gate_total_held": total,
        "ret_gate_total": fl._product(total, q),
        "sgov_tr": sgov,
    }


def _synthetic(
    n: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    days = fl.trading_days(date(2026, 11, 24), date(2028, 6, 30))[:n]
    main, gate = [], []
    for k, day in enumerate(days):
        q = None if k == 3 else 0.001 * ((k * 7) % 11 - 5)
        row = _main_row(k + 40, day, q, 1.0)
        main.append(row)
        band = (0.2, 0.4) if k < 5 else (0.4, 0.4)
        if k == 6:
            gate.append(_gate_row(row, None, mode=None, sgov=None))
        elif k == 7:
            gate.append(_gate_row(row, band, mode="gated_min"))
        else:
            gate.append(_gate_row(row, band, sgov=None if k == 8 else 0.0002))
    return main, gate


def _summary_cells(returns: list[float | None], exposures: list[float | None]) -> str:
    wealth, peak, drawdown = 1.0, 1.0, 0.0
    for value in returns:
        if value is not None:
            wealth *= 1.0 + value
            peak = max(peak, wealth)
            drawdown = min(drawdown, wealth / peak - 1.0)
    present = [e for e in exposures if e is not None]
    mean = f"{sum(present) / len(present):.2f}" if present else "—"
    missing = sum(value is None for value in returns)
    return (
        f"| {len(returns)} | {missing} | {(wealth - 1.0) * 100:+.2f}% | "
        f"{drawdown * 100:+.2f}% | {mean} |"
    )


def test_the_section_prints_the_table_and_the_lines(monkeypatch: pytest.MonkeyPatch) -> None:
    main, gate = _synthetic(20)
    main_rows = [_main_row(k + 1, date(2026, 10, 12), 0.0, 0.4) for k in range(39)] + main
    lines = fg.render_section(main_rows, gate)
    text = "\n".join(lines)
    q = [row["qqq_tr"] for row in main]
    e = [row["exp_gate_held"] for row in gate]
    r = [row["ret_gate"] for row in gate]
    matched = [k for k, value in enumerate(r) if value is not None]
    mean = sum(e[k] for k in matched) / len(matched)
    fixed = [mean * q[k] for k in matched]
    expected_rows = {
        "闸门线（提示）": _summary_cells(r, e),
        "同平均敞口的固定（闸门线的对照）": _summary_cells(fixed, [mean] * len(matched)),
        "买入持有": _summary_cells(q, [1.0] * len(q)),
        "日报线（主账本，满仓为底）": _summary_cells(
            [row["ret_daily"] for row in main], [row["exp_daily_held"] for row in main]
        ),
        "固定 0.8（主账本）": _summary_cells(
            [row["ret_fixed"] for row in main], [row["exp_fixed"] for row in main]
        ),
        "F0001（主账本）": _summary_cells(
            [row["ret_overlay"] for row in main], [row["exp_overlay"] for row in main]
        ),
        "闸门总资产口径（只印）": _summary_cells(
            [row["ret_gate_total"] for row in gate], [row["exp_gate_total_held"] for row in gate]
        ),
    }
    assert "| 线 | 交易日 | 缺收益 | 累计 | 最大回撤 | 平均敞口 |" in lines
    order = [text.index(f"| {label} | {cells[2:]}") for label, cells in expected_rows.items()]
    assert order == sorted(order)
    pairs = [(r[k], fixed[i], q[k]) for i, k in enumerate(matched)]
    own = sum(math.log1p(p[0]) for p in pairs)
    versus_fixed = (own - sum(math.log1p(p[1]) for p in pairs)) * 100
    versus_hold = (own - sum(math.log1p(p[2]) for p in pairs)) * 100
    assert (
        f"闸门线对同平均敞口固定的累计对数差 {versus_fixed:+.2f} 点，"
        f"对买入持有 {versus_hold:+.2f} 点（{len(pairs)} 个交易日）。不满一年，不印年化。"
    ) in lines
    assert DECISION in text and fl.POLICY_DECISION in text
    # Named modes in order of first appearance, then the rows without one.
    assert (
        "持有快照的仓位方式：full_exposure_default 18 行；gated_min 1 行；没有快照 1 行。" in lines
    )
    assert "闸门区间变了的日子（共 1 次）：2026-12-02 0.20–0.40 → 0.40–0.40（防守）。" in lines
    assert lines[-1] == "闸门线落后主账本 0 行。"


def test_the_sgov_line_counts_missing_cash(monkeypatch: pytest.MonkeyPatch) -> None:
    main, gate = _synthetic(12)
    lines = fg.render_section(main, gate)
    e = [row["exp_gate_held"] for row in gate]
    q = [row["qqq_tr"] for row in gate]
    s = [row["sgov_tr"] for row in gate]
    own = [
        None if ek is None or qk is None or sk is None else ek * qk + (1 - ek) * sk
        for ek, qk, sk in zip(e, q, s, strict=True)
    ]
    matched = [k for k, row in enumerate(gate) if row["ret_gate"] is not None]
    mean = sum(e[k] for k in matched) / len(matched)
    other = [
        None if q[k] is None or s[k] is None else mean * q[k] + (1 - mean) * s[k] for k in matched
    ]

    def cumulative(values: list[float | None]) -> float:
        wealth = 1.0
        for value in values:
            if value is not None:
                wealth *= 1.0 + value
        return wealth - 1.0

    missing = sum(value is None for value in s)
    assert (
        f"现金按同一价格文件里的 SGOV（只印）：闸门线累计 {cumulative(own) * 100:+.2f}%，"
        f"同平均敞口的固定 {cumulative(other) * 100:+.2f}%（缺 SGOV {missing} 天）。"
    ) in lines


def test_the_annual_difference_needs_252_return_days() -> None:
    main, gate = _synthetic(253)  # one day without a QQQ return, one without a snapshot
    assert "不满一年，不印年化。" in "\n".join(fg.render_section(main, gate))
    main, gate = _synthetic(254)
    text = "\n".join(fg.render_section(main, gate))
    assert "几何年化差：对同平均敞口固定 " in text and " 点/年，对买入持有 " in text
    assert "不满一年，不印年化。" not in text


def test_the_section_without_rows_and_the_lag() -> None:
    main, _ = _synthetic(4)
    lines = fg.render_section(main, [])
    assert lines[1] == f"{fg.SECTION}（自 2026-11-24，只记账，不判）"
    assert "闸门线自 2026-11-24 起记，还没有行。" in lines
    assert lines[-1] == "闸门线落后主账本 4 行。"
    assert not any(line.startswith("| 线 |") for line in lines)
    main, gate = _synthetic(6)
    assert fg.render_section(main, gate[:4])[-1] == "闸门线落后主账本 2 行。"


def test_no_band_change_says_none() -> None:
    main, gate = _synthetic(4)
    assert "闸门区间变了的日子：没有。" in fg.render_section(main, gate)


def test_band_changes_skip_rows_without_a_band_and_print_the_last_ten() -> None:
    main, gate = _synthetic(8)
    assert gate[6]["gate_min"] is None  # rows 5 and 7 differ across a row without a band
    gate[7].update(gate_min=0.6, gate_max=0.7, gate_label="中性")
    assert (
        f"闸门区间变了的日子（共 2 次）：{gate[5]['date']} 0.20–0.40 → 0.40–0.40（防守）；"
        f"{gate[7]['date']} 0.40–0.40 → 0.60–0.70（中性）。"
    ) in fg.render_section(main, gate)
    main, gate = _synthetic(30)
    for k, row in enumerate(gate):
        if row["gate_min"] is not None:
            row.update(gate_min=0.2 if k % 2 else 0.4, gate_max=0.4)
    # Changes on rows 1-5 and 8-29 (rows 5 and 7 hold the same band across row 6): 27.
    latest = []
    for k in range(20, 30):
        before, after = (0.4, 0.2) if k % 2 else (0.2, 0.4)
        latest.append(f"{gate[k]['date']} {before:.2f}–0.40 → {after:.2f}–0.40（防守）")
    assert (
        "闸门区间变了的日子（共 27 次，印最近 10 次）：" + "；".join(latest) + "。"
    ) in fg.render_section(main, gate)


def test_a_broken_gate_file_makes_weekly_exit_one(
    written: list[dict[str, Any]], ledger_dir: Path
) -> None:
    rows = [dict(row) for row in written]
    rows[0]["ret_gate"] = 1.0
    _write_rows(ledger_dir, rows)
    result = CliRunner().invoke(fl.app, ["weekly", "--ledger-dir", str(ledger_dir)])
    assert result.exit_code == 1
    assert "forward_ledger: 周汇总已写" in result.output
    assert "forward_gate: 周汇总没有加「闸门线（提示）」一节：" in result.output
    assert fg.SECTION not in (ledger_dir / fl.WEEKLY_FILE).read_text(encoding="utf-8")


def test_the_weekly_command_adds_the_section_and_says_so(
    written: list[dict[str, Any]], ledger_dir: Path
) -> None:
    result = CliRunner().invoke(fl.app, ["weekly", "--ledger-dir", str(ledger_dir)])
    assert result.exit_code == 0, result.output
    assert "forward_gate: 周汇总加了「闸门线（提示）」一节" in result.output
    text = (ledger_dir / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    assert text.count(fg.SECTION) == 1 and text.endswith(FOOTER)
    assert f"| 闸门线（提示） | {len(written)} |" in text
