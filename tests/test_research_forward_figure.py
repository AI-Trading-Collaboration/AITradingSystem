"""TRADING-2565-48: the forward ledger's figure (SVG, read only); TRADING-2565-60 adds the lines."""

from __future__ import annotations

import math
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system.research_probes import forward_figure as ff
from ai_trading_system.research_probes import forward_ledger as fl

SVG = "{http://www.w3.org/2000/svg}"


def _rows() -> list[dict[str, Any]]:
    days = ["2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16", "2026-10-19"]
    qqq = [0.01, -0.02, None, 0.015, 0.0, 0.005]
    held = [0.7, 0.7, None, 0.65, 0.65, 0.6]
    overlay = [1.0, 1.0, 1.0, 0.5, 0.5, 0.5]
    return [
        {
            "date": day,
            "qqq_tr": r,
            "exp_daily_held": h,
            "exp_fixed": 0.8,
            "exp_overlay": o,
            "ret_daily": None if r is None or h is None else h * r,
            "ret_fixed": None if r is None else 0.8 * r,
            "ret_overlay": None if r is None else o * r,
            "prev_sha256": None if k == 0 else f"{k:02d}" + "ab" * 31,
        }
        for k, (day, r, h, o) in enumerate(zip(days, qqq, held, overlay, strict=True))
    ]


def test_cumulative_compounds_and_leaves_a_gap() -> None:
    level = ff.cumulative(_rows())
    assert [d.strftime("%Y-%m-%d") for d in level.index] == [row["date"] for row in _rows()]
    values = level.to_list()
    assert values[0] == pytest.approx(1.01)
    assert values[1] == pytest.approx(1.01 * 0.98)
    assert math.isnan(values[2])
    assert values[3] == pytest.approx(1.01 * 0.98 * 1.015)
    assert values[5] == pytest.approx(1.01 * 0.98 * 1.015 * 1.005)


def _root(data: bytes) -> ET.Element:
    return ET.fromstring(data)


def test_render_draws_runs_three_exposures_and_a_footer() -> None:
    root = _root(ff.render(_rows()))
    lines = [p for p in root.iter(f"{SVG}polyline")]
    assert sum(1 for p in lines if p.get("class") == "price") == 2
    exposure = [p for p in lines if p.get("class") == "exposure"]
    assert [p.get("stroke") for p in exposure] == ["#d62728", "#d62728", "#8c8c8c", "#1f77b4"]
    text = " ".join(t.text or "" for t in root.iter(f"{SVG}text"))
    assert "6 行（2026-10-12..2026-10-19，回填到 2026-10-21 之前）" in text
    assert "最后一行 prev_sha256 05ababababab；只读账本，不判。" in text
    assert "日报持有敞口" in text and "固定 0.8" in text and "覆盖 F0001 fast_slow" in text


def test_no_text_inside_the_plot_areas() -> None:
    root = _root(ff.render(_rows()))
    areas = [
        (float(r.get("x")), float(r.get("y")), float(r.get("width")), float(r.get("height")))
        for r in root.iter(f"{SVG}rect")
        if r.get("class") == "plot-area"
    ]
    assert len(areas) == 3
    for text in root.iter(f"{SVG}text"):
        x, y = float(text.get("x")), float(text.get("y"))
        for ax, ay, aw, ah in areas:
            assert not (ax < x < ax + aw and ay < y < ay + ah), text.text


def test_an_empty_ledger_is_refused() -> None:
    with pytest.raises(ValueError, match="empty"):
        ff.render([])


def test_draw_writes_once_and_reports_changes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    rows = _rows()
    monkeypatch.setattr(fl, "read_ledger", lambda ledger_dir: rows)
    path, changed = ff.draw(tmp_path / "ledger", tmp_path / "figures")
    assert path == tmp_path / "figures" / ff.FIGURE and changed
    first = path.read_bytes()
    assert first == ff.render(rows)
    assert ff.draw(tmp_path / "ledger", tmp_path / "figures") == (path, False)
    rows.append({**rows[-1], "date": "2026-10-20", "prev_sha256": "06" + "cd" * 31})
    assert ff.draw(tmp_path / "ledger", tmp_path / "figures") == (path, True)
    assert path.read_bytes() != first


def test_the_command_refuses_a_broken_or_empty_ledger(tmp_path: Path) -> None:
    runner = CliRunner()
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / fl.LEDGER_FILE).write_text("not json\n", encoding="utf-8")
    found = runner.invoke(
        ff.app, ["draw", "--ledger-dir", str(broken), "--out", str(tmp_path / "out")]
    )
    assert found.exit_code == 1
    assert "forward_figure: the ledger is broken" in found.output
    empty = tmp_path / "empty"
    empty.mkdir()
    found = runner.invoke(
        ff.app, ["draw", "--ledger-dir", str(empty), "--out", str(tmp_path / "out")]
    )
    assert found.exit_code == 1 and "the ledger is empty" in found.output
    assert not (tmp_path / "out" / ff.FIGURE).exists()


def test_the_command_says_whether_it_changed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(fl, "read_ledger", lambda ledger_dir: _rows())
    runner = CliRunner()
    found = runner.invoke(ff.app, ["draw", "--out", str(tmp_path)])
    assert found.exit_code == 0 and "forward_figure: 已更新" in found.output
    found = runner.invoke(ff.app, ["draw", "--out", str(tmp_path)])
    assert found.exit_code == 0 and "forward_figure: 没有变化" in found.output


def test_the_default_output_sits_beside_the_ledger() -> None:
    assert ff.DEFAULT_OUT == fl.DEFAULT_LEDGER_DIR / "figures"


# The four lines' cumulative panel (TRADING-2565-60) ----------------------------------------------


def _forward_rows() -> list[dict[str, Any]]:
    later = []
    for k, (day, r) in enumerate((("2026-10-21", 0.02), ("2026-10-22", -0.01))):
        later.append(
            {
                "date": day,
                "qqq_tr": r,
                "exp_daily_held": 0.6,
                "exp_fixed": 0.8,
                "exp_overlay": 0.5,
                "ret_daily": 0.6 * r,
                "ret_fixed": 0.8 * r,
                "ret_overlay": 0.5 * r,
                "prev_sha256": f"{k + 6:02d}" + "ef" * 31,
            }
        )
    rows = _rows()
    rows.insert(len(rows), {**rows[-1], "date": "2026-10-20", "prev_sha256": "06" + "cd" * 31})
    return rows + later


def test_line_levels_compound_each_line_and_the_matched_fixed() -> None:
    rows = _rows()
    levels = ff.line_levels(rows)
    assert list(levels) == ["daily", "fixed", "overlay", "matched"]
    for series in levels.values():
        assert [d.strftime("%Y-%m-%d") for d in series.index] == [row["date"] for row in rows]
    assert levels["daily"].iloc[1] == pytest.approx(1.007 * (1 - 0.014))
    assert math.isnan(levels["daily"].iloc[2])
    assert levels["fixed"].iloc[3] == pytest.approx(1.008 * (1 - 0.016) * 1.012)
    assert levels["overlay"].iloc[5] == pytest.approx(1.01 * 0.98 * 1.0075 * 1.0 * 1.0025)
    # Six overlay exposures (1, 1, 1, 0.5, 0.5, 0.5), but 10-14 has no return: mean of five = 0.8.
    mean = (1.0 + 1.0 + 0.5 + 0.5 + 0.5) / 5
    assert levels["matched"].iloc[0] == pytest.approx(1 + mean * 0.01)
    assert math.isnan(levels["matched"].iloc[2])
    assert levels["matched"].iloc[5] == pytest.approx(
        (1 + mean * 0.01) * (1 - mean * 0.02) * (1 + mean * 0.015) * 1.0 * (1 + mean * 0.005)
    )


def test_the_middle_panel_draws_four_lines_and_reads_both_gaps() -> None:
    rows = _forward_rows()
    root = _root(ff.render(rows))
    cumulative = [p for p in root.iter(f"{SVG}polyline") if p.get("class") == "cumulative"]
    colors = {p.get("stroke") for p in cumulative}
    assert colors == {"#d62728", "#8c8c8c", "#1f77b4", "#9467bd"}
    text = [t.text or "" for t in root.iter(f"{SVG}text")]
    assert text[0] == "前瞻账本：QQQ 累计总回报、四条线的累计和三条敞口"
    assert "四条线的累计（现金 0，起点前一天 = 1，缺的日子断开；虚线是前瞻起点 2026-10-21）" in text
    backfill = fl.overlay_difference(rows, fl.BACKFILL_START)
    forward = fl.overlay_difference(rows, fl.FORWARD_START)
    assert (
        "F0001 对同平均敞口固定的累计对数差："
        f"自 2026-10-12 {backfill['log'] * 100:+.2f} 点，"
        f"自 2026-10-21 {forward['log'] * 100:+.2f} 点"
    ) in text
    assert (
        "（对照线按自 2026-10-12 的平均敞口画；自 2026-10-21 的数用 2026-10-21 起的平均）" in text
    )
    for label in ("日报模型", "固定 0.8", "F0001 fast_slow 覆盖", "F0001 的同平均敞口固定"):
        assert label in text
    (dashed,) = [line for line in root.iter(f"{SVG}line") if line.get("class") == "forward-start"]
    assert dashed.get("x1") == dashed.get("x2") and dashed.get("stroke-dasharray")
    middle = sorted(
        (float(r.get("y")), r) for r in root.iter(f"{SVG}rect") if r.get("class") == "plot-area"
    )[1][1]
    top, height = float(middle.get("y")), float(middle.get("height"))
    assert (float(dashed.get("y1")), float(dashed.get("y2"))) == (top, top + height)


def test_before_the_forward_start_there_is_no_dashed_line_and_no_forward_gap() -> None:
    root = _root(ff.render(_rows()))
    assert not [line for line in root.iter(f"{SVG}line") if line.get("class") == "forward-start"]
    text = " ".join(t.text or "" for t in root.iter(f"{SVG}text"))
    assert "自 2026-10-21 还没有行" in text


def test_the_figure_is_the_same_bytes_for_the_same_rows() -> None:
    assert ff.render(_forward_rows()) == ff.render(_forward_rows())
