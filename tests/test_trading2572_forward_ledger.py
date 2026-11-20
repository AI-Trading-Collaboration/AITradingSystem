"""TRADING-2572: the forward ledger's weekly gains a full-exposure row and splits the daily line.

Synthetic files only. The daily switched to full exposure as the base (owner_decision
TRADING-2572 2026-11-19 full_exposure_default); the ledger's rows and schema stay as they are.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from test_research_forward_ledger import (
    LAST_DAY,
    NO_CAPTURE,
    NO_SNAPSHOT,
    PROVENANCE,
    _band,
    _row,
    _write_capture,
    _write_snapshot,
)
from typer.testing import CliRunner

from ai_trading_system.research_probes import forward_added, forward_frozen
from ai_trading_system.research_probes import forward_ledger as fl

# The fixture's switch: old snapshots (no position_policy block) through 10-26, a gated_min block
# on 10-27, full exposure from 10-28, and no snapshot on 10-29.
GATED_DAY = date(2026, 10, 27)
FIRST_FULL = date(2026, 10, 28)
MISSING_FULL = date(2026, 10, 29)
FIRST_AFTER = "2026-10-30"
# No snapshot on 10-08 either, so the first row (10-12) holds nothing and has no daily return.
NO_HELD = date(2026, 10, 8)
FULL_BAND = (1.0, 1.0, "满仓（默认）", 0.4, 0.6)


def _add_policy(root: Path, day: date, mode: str) -> None:
    path = root / fl.SNAPSHOT_REL.format(day=day.isoformat())
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    positions = snapshot["positions"]
    positions["position_policy"] = {
        "mode": mode,
        "owner_decision": fl.POLICY_DECISION if mode == fl.POLICY_FULL else None,
    }
    positions["gated_risk_asset_ai_band"] = {"min_position": 0.4, "max_position": 0.4, "label": "x"}
    path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")


def _switching_root(root: Path, *, full_from: date | None = FIRST_FULL) -> Path:
    for day in fl.trading_days(date(2026, 10, 8), LAST_DAY):
        if day != NO_CAPTURE:
            _write_capture(root, day)
        if day in (NO_SNAPSHOT, MISSING_FULL, NO_HELD):
            continue
        full = full_from is not None and day >= full_from
        _write_snapshot(root, day, FULL_BAND if full else _band(day))
        if full:
            _add_policy(root, day, fl.POLICY_FULL)
        elif day >= GATED_DAY:
            _add_policy(root, day, "gated_min")
    return root


@pytest.fixture()
def switching(tmp_path: Path) -> tuple[Path, Path]:
    root = _switching_root(tmp_path / "daily")
    ledger_dir = tmp_path / "forward"
    fl.append(ledger_dir, root, LAST_DAY, provenance=PROVENANCE)
    return root, ledger_dir


def _summary_row(label: str, summary: dict) -> str:
    return (
        f"| {label} | {summary['days']} | {summary['missing']} | "
        f"{summary['cumulative'] * 100:+.2f}% | {summary['max_drawdown'] * 100:+.2f}% | "
        f"{summary['mean_exposure']:.2f} |"
    )


def test_constants_name_the_decision_and_the_labels() -> None:
    assert fl.POLICY_FULL == "full_exposure_default"
    assert fl.POLICY_DECISION == "owner_decision:TRADING-2572:2026-11-19:full_exposure_default"
    assert fl.FULL_LABEL.format(since=date(2026, 10, 12)) == (
        "满仓 1.0（= QQQ 持有，自 2026-10-12 起）"
    )
    assert fl.BEFORE_LABEL.format(span="a..b") == "日报模型（换包前，a..b）"
    assert fl.AFTER_LABEL.format(span="还没有") == "日报模型（换包后，还没有）"
    assert fl.BEFORE_MATCHED_LABEL == "同平均敞口的固定（换包前日报模型的对照）"
    assert fl.NO_SNAPSHOTS == "没有读快照（`weekly` 没给 `--daily-root`），不分段。"


def test_the_full_row_is_exposure_one_on_the_ledgers_own_qqq_returns() -> None:
    rows = [
        _row("2026-10-20", 0.4, 0.10, 1.0),
        _row("2026-10-21", 0.4, None, 1.0),
        _row("2026-10-22", 0.4, -0.20, 0.5),
        _row("2026-10-23", 0.4, 0.05, 0.5),
    ]
    full = fl.full_summary(rows, date(2026, 10, 12))
    assert (full["days"], full["missing"], full["mean_exposure"]) == (4, 1, 1.0)
    assert full["cumulative"] == pytest.approx(1.10 * 0.80 * 1.05 - 1, abs=1e-15)
    assert full["max_drawdown"] == pytest.approx(-0.20, abs=1e-15)
    later = fl.full_summary(rows, date(2026, 10, 21))
    assert (later["days"], later["missing"]) == (3, 1)
    assert later["cumulative"] == pytest.approx(0.80 * 1.05 - 1, abs=1e-15)
    lines = fl.render_weekly(rows).split("\n")
    for since, summary, matched_days in (
        (date(2026, 10, 12), full, 3),
        (date(2026, 10, 21), later, 2),
    ):
        row = _summary_row(fl.FULL_LABEL.format(since=since), summary)
        at = lines.index(row)
        # Last in its table, after the F0001 matched row, before the blank line.
        assert lines[at - 1].startswith(f"| 同平均敞口的固定（F0001 的对照） | {matched_days} |")
        assert lines[at + 1] == ""
    assert sum(line.startswith("| 满仓 1.0（= QQQ 持有，") for line in lines) == 2


def test_policy_mode_reads_the_snapshots_policy_block(tmp_path: Path) -> None:
    day = date(2026, 10, 28)
    assert fl.read_policy_mode(tmp_path, day) is None
    _write_snapshot(tmp_path, day, _band(day))
    assert fl.read_policy_mode(tmp_path, day) == ""
    _add_policy(tmp_path, day, "gated_min")
    assert fl.read_policy_mode(tmp_path, day) == "gated_min"
    _add_policy(tmp_path, day, fl.POLICY_FULL)
    assert fl.read_policy_mode(tmp_path, day) == fl.POLICY_FULL
    (tmp_path / fl.SNAPSHOT_REL.format(day=day.isoformat())).write_text("{", encoding="utf-8")
    with pytest.raises(fl.ForwardLedgerError, match="decision_snapshot_2026-10-28.json"):
        fl.read_policy_mode(tmp_path, day)


def test_the_switch_row_holds_the_first_full_snapshot_two_trading_days_later(
    switching: tuple[Path, Path],
) -> None:
    root, ledger_dir = switching
    rows = fl.read_ledger(ledger_dir)
    # A gated_min block on 10-27 is not the switch: the mode decides, not the block.
    switch = fl.policy_switch(root, rows)
    assert switch == fl.PolicySwitch("2026-10-28", FIRST_AFTER)
    by_date = {row["date"]: row for row in rows}
    # 10-29 still holds 10-27's band (the gated_min block), not a full one.
    low, high, *_ = _band(GATED_DAY)
    assert by_date["2026-10-29"]["exp_daily_held"] == pytest.approx((low + high) / 2.0)
    assert by_date[FIRST_AFTER]["exp_daily_held"] == 1.0
    # 11-02 holds 10-28 too, because 10-29 has no snapshot.
    assert by_date["2026-11-02"]["exp_daily_held"] == 1.0
    assert all(row["exp_daily_held"] == 1.0 for row in rows if row["date"] >= FIRST_AFTER)
    assert fl.read_weekly_capture(root, rows).switch == switch


def test_the_weekly_prints_both_segments_and_the_rule(switching: tuple[Path, Path]) -> None:
    root, ledger_dir = switching
    text = fl.write_weekly(ledger_dir, root).read_text(encoding="utf-8")
    rows = fl.read_ledger(ledger_dir)
    cut = [row["date"] for row in rows].index(FIRST_AFTER)
    lines = text.split("\n")
    start = lines.index("## 日报线换包前后（自 2026-10-12）")
    assert lines[start + 2] == (
        "判据：日报线持有的那份快照里 `positions.position_policy.mode` 是 `full_exposure_default`"
        "（`owner_decision:TRADING-2572:2026-11-19:full_exposure_default`）。"
        "日报线 t+2 持有：第一份满仓快照 as_of 2026-10-28，持有它的第一行 2026-10-30，"
        "从这一行起是换包后。"
    )
    assert lines[start + 3] == (
        "F0005 写的是换包前的日报模型，只读换包前那段，不改它的数；换包后的日报线就是满仓 1.0。"
    )
    before = fl.summarize(rows[:cut], fl.BACKFILL_START)["daily"]
    control = fl.summarize(rows[:cut], fl.BACKFILL_START)["fixed_at_daily_mean"]
    after = fl.summarize(rows[cut:], fl.BACKFILL_START)["daily"]
    assert lines[start + 4 : start + 10] == [
        "",
        "| 线 | 交易日 | 缺收益 | 累计 | 最大回撤 | 平均敞口 |",
        "|---|---|---|---|---|---|",
        _summary_row("日报模型（换包前，2026-10-12..2026-10-29）", before),
        _summary_row("同平均敞口的固定（换包前日报模型的对照）", control),
        _summary_row(f"日报模型（换包后，{FIRST_AFTER}..2026-11-06）", after),
    ]
    # F0005's control: the "before" daily line's mean held exposure over its rows with a return,
    # not the main table's control, which mixes in the full rows.
    held = [row["exp_daily_held"] for row in rows[:cut] if row["ret_daily"] is not None]
    assert rows[0]["ret_daily"] is None and len(held) == cut - 1
    assert control["mean_exposure"] == pytest.approx(sum(held) / len(held), abs=1e-15)
    assert control["days"] == len(held)
    mixed = fl.summarize(rows, fl.BACKFILL_START)["fixed_at_daily_mean"]["mean_exposure"]
    assert mixed > control["mean_exposure"] + 0.05
    assert after["mean_exposure"] == 1.0
    assert before["days"] + after["days"] == len(rows)
    # The segments close the main ledger's part, before the footer.
    assert lines.index("## 按周") < start
    assert lines[start + 10 :] == ["", "production_effect=none；broker_action=none。", ""]


def test_no_full_snapshot_yet_keeps_every_row_before(tmp_path: Path) -> None:
    root = _switching_root(tmp_path / "daily", full_from=None)
    ledger_dir = tmp_path / "forward"
    fl.append(ledger_dir, root, LAST_DAY, provenance=PROVENANCE)
    rows = fl.read_ledger(ledger_dir)
    assert fl.policy_switch(root, rows) is None
    text = fl.write_weekly(ledger_dir, root).read_text(encoding="utf-8")
    assert "日报线持有的快照还没有满仓的，全部是换包前。" in text
    before = fl.summarize(rows, fl.BACKFILL_START)["daily"]
    span = f"{rows[0]['date']}..{rows[-1]['date']}"
    assert _summary_row(f"日报模型（换包前，{span}）", before) in text
    control = fl.summarize(rows, fl.BACKFILL_START)["fixed_at_daily_mean"]
    assert _summary_row("同平均敞口的固定（换包前日报模型的对照）", control) in text
    assert "| 日报模型（换包后，还没有） | 0 | 0 | — | — | — |" in text
    assert fl.NO_SNAPSHOTS in fl.render_weekly(rows)


def test_the_weekly_never_changes_the_ledger_rows(switching: tuple[Path, Path]) -> None:
    root, ledger_dir = switching
    ledger = ledger_dir / fl.LEDGER_FILE
    before = ledger.read_bytes()
    fl.write_weekly(ledger_dir, root)
    fl.write_weekly(ledger_dir)
    assert ledger.read_bytes() == before
    assert fl.verify(ledger_dir) == []
    for line in before.splitlines():
        row = json.loads(line)
        assert set(row) == fl.ROW_KEYS
        assert row["schema_version"] == fl.SCHEMA


def test_weekly_command_orders_the_sections(switching: tuple[Path, Path]) -> None:
    root, ledger_dir = switching
    result = CliRunner().invoke(
        fl.app, ["weekly", "--ledger-dir", str(ledger_dir), "--daily-root", str(root)]
    )
    assert result.exit_code == 0, result.output
    text = (ledger_dir / fl.WEEKLY_FILE).read_text(encoding="utf-8")
    order = [
        text.index("## 自 2026-10-12（含回填）"),
        text.index("| 满仓 1.0（= QQQ 持有，自 2026-10-12 起） |"),
        text.index("## 自 2026-10-21（前瞻）"),
        text.index("| 满仓 1.0（= QQQ 持有，自 2026-10-21 起） |"),
        text.index("## 按周"),
        text.index("## 日报线换包前后（自 2026-10-12）"),
        text.index(forward_added.SECTION),
        text.index(forward_frozen.SECTION),
        text.index("production_effect=none；broker_action=none。"),
    ]
    assert order == sorted(order)
