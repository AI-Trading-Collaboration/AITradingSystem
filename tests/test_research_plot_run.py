"""TRADING-2565-48: entry and exit figures (SVG, standard library) on synthetic series."""

from __future__ import annotations

import hashlib
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.research_probes import plot_run as pr

NS = {"s": "http://www.w3.org/2000/svg"}


def _legs(index: pd.DatetimeIndex, seed: int, step: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    fast = np.repeat(rng.integers(0, 2, len(index) // step + 1), step)[: len(index)]
    slow = np.repeat(rng.integers(0, 2, len(index) // (3 * step) + 1), 3 * step)[: len(index)]
    return pd.DataFrame({"fast": fast.astype(float), "slow": slow.astype(float)}, index=index)


def _spec(step: str = "run") -> pr.FigureSpec:
    index = pd.bdate_range("1999-03-11", "2010-12-31")
    rng = np.random.default_rng(11)
    asset = pd.Series(rng.normal(0.0003, 0.015, len(index)), index=index)
    rules = []
    for role, seed, name in (("base", 1, "月频 fast_slow"), ("compared", 2, "H-A 不对称再入")):
        legs = _legs(index, seed, 21 if role == "base" else 9)
        exposure = 0.5 * legs["fast"] + 0.5 * legs["slow"]
        matched = float(exposure.mean()) * asset
        rules.append(pr.RuleSeries(name, role, exposure, exposure * asset, legs, matched))  # type: ignore[arg-type]
    segments = [
        pr.SegmentBand("slow_bear_200003", "slow_bear", "2000-04", "2002-09"),
        pr.SegmentBand("slow_bear_200710", "slow_bear", "2007-11", "2009-02"),
        pr.SegmentBand("long_bull_200210", "long_bull", "2002-10", "2007-10"),
        pr.SegmentBand("fast_drop_201004", "fast_drop", "2010-05", "2010-06"),
    ]
    readings = {
        "slow_bear_200003": {
            "base": pr.SegmentReading(0.119, 20, 300),
            "compared": pr.SegmentReading(0.08, 20, None),
        },
        "long_bull_200210": {
            "base": pr.SegmentReading(-0.033),
            "compared": pr.SegmentReading(-0.012),
        },
    }
    return pr.FigureSpec(
        step=step,
        market="qqq",
        title="QQQ 1999–2010，合成",
        window_id="qqq_long_history_development",
        asset_returns=asset,
        segments=segments,
        rules=rules,
        readings=readings,
        footer=["脚注一", "脚注二"],
    )


def _root(path: Path) -> ET.Element:
    return ET.parse(path).getroot()


def _areas(root: ET.Element) -> list[tuple[float, float, float, float]]:
    return [
        (float(r.get("x")), float(r.get("y")), float(r.get("width")), float(r.get("height")))
        for r in root.iter(f"{{{NS['s']}}}rect")
        if r.get("class") == "plot-area"
    ]


def test_leg_moves_dates_kinds_and_sizes() -> None:
    index = pd.bdate_range("2001-01-01", periods=6)
    legs = pd.DataFrame(
        {"fast": [1.0, 0.0, 0.0, 1.0, 0.5, 1.0], "slow": [0.0, 0.0, 1.0, 1.0, 1.0, 0.0]},
        index=index,
    )
    moves = pr.leg_moves(legs)
    assert list(moves.columns) == ["date", "leg", "kind", "size"]
    got = [(m.date, m.leg, m.kind, m.size) for m in moves.itertuples()]
    assert got == [
        (index[1], "fast", "exit", 1.0),
        (index[2], "slow", "entry", 1.0),
        (index[3], "fast", "entry", 1.0),
        (index[4], "fast", "exit", 0.5),
        (index[5], "fast", "entry", 0.5),
        (index[5], "slow", "exit", 1.0),
    ]
    counts = pr.count_moves(moves)
    assert counts == {"slow": {"exit": 1, "entry": 1}, "fast": {"exit": 2, "entry": 2}}


def test_count_moves_by_effective_month() -> None:
    index = pd.bdate_range("2001-01-29", "2001-04-03")
    fast = pd.Series(1.0, index=index)
    fast.loc["2001-02-01":"2001-03-14"] = 0.0
    legs = pd.DataFrame({"fast": fast, "slow": 1.0}, index=index)
    moves = pr.leg_moves(legs)
    assert pr.count_moves(moves, "2001-02", "2001-02") == {
        "slow": {"exit": 0, "entry": 0},
        "fast": {"exit": 1, "entry": 0},
    }
    assert pr.count_moves(moves, "2001-03", None)["fast"] == {"exit": 0, "entry": 1}
    assert pr.count_moves(moves, None, "2001-01")["fast"] == {"exit": 0, "entry": 0}


def test_figures_written_hashed_reproducible_and_never_overwritten(tmp_path: Path) -> None:
    spec = _spec()
    records = pr.figures(spec, tmp_path / "a")
    assert [r.name for r in records] == ["run-qqq-A.svg", "run-qqq-B.svg", "run-qqq-C.svg"]
    for record in records:
        data = (tmp_path / "a" / record.name).read_bytes()
        assert record.sha256 == hashlib.sha256(data).hexdigest()
        assert record.bytes == len(data)
        assert data.startswith(b"<?xml")
        _root(tmp_path / "a" / record.name)
    again = pr.figures(spec, tmp_path / "b")
    assert [r.sha256 for r in again] == [r.sha256 for r in records]
    with pytest.raises(FileExistsError):
        pr.figures(spec, tmp_path / "a")


def test_no_text_inside_any_plot_area(tmp_path: Path) -> None:
    for record in pr.figures(_spec(), tmp_path):
        root = _root(tmp_path / record.name)
        areas = _areas(root)
        assert areas
        texts = list(root.iter(f"{{{NS['s']}}}text"))
        assert texts
        for text in texts:
            x, y = float(text.get("x")), float(text.get("y"))
            for ax, ay, aw, ah in areas:
                assert not (ax < x < ax + aw and ay < y < ay + ah), (record.name, text.text)


def _stems(root: ET.Element) -> list[ET.Element]:
    return [g for g in root.iter(f"{{{NS['s']}}}g") if g.get("class") == "stems"]


def test_figure_a_stems_match_counts_and_point_the_right_way(tmp_path: Path) -> None:
    spec = _spec()
    record = pr.figures(spec, tmp_path)[0]
    root = _root(tmp_path / record.name)
    lengths: dict[tuple[str, str], set[float]] = {}
    for group in _stems(root):
        role, leg, kind = group.get("data-rule"), group.get("data-leg"), group.get("data-kind")
        lines = group.findall("s:line", NS)
        assert len(group.findall("s:polygon", NS)) == len(lines)
        assert len(lines) == record.counts["window"][role][leg][kind]
        assert len(lines) == record.counts["drawn"][role][leg][kind]
        for line in lines:
            y1, y2 = float(line.get("y1")), float(line.get("y2"))
            assert (y2 < y1) if kind == "exit" else (y2 > y1)
            lengths.setdefault((role, leg), set()).add(round(abs(y2 - y1), 1))
    rules = {rule.role: rule for rule in spec.rules}
    for role, rule in rules.items():
        assert record.counts["window"][role] == pr.count_moves(pr.leg_moves(rule.legs))
    one = {key: next(iter(value)) for key, value in lengths.items()}
    assert all(len(value) == 1 for value in lengths.values())
    assert (
        one[("base", "slow")]
        > one[("compared", "slow")]
        > one[("base", "fast")]
        > one[("compared", "fast")]
    )


def test_figure_b_one_panel_per_slow_bear_with_its_readings(tmp_path: Path) -> None:
    spec = _spec()
    record = pr.figures(spec, tmp_path)[1]
    root = _root(tmp_path / record.name)
    assert list(record.counts["panels"]) == ["slow_bear_200003", "slow_bear_200710"]
    assert len(_areas(root)) == 2
    text = " ".join(t.text or "" for t in root.iter(f"{{{NS['s']}}}text"))
    assert "D +11.9 点，出场滞后 20 天，回仓滞后 300 天" in text
    assert "D +8.0 点，出场滞后 20 天，回仓滞后 —" in text
    drawn = [len(g.findall("s:line", NS)) for g in _stems(root)]
    expected = [
        record.counts["panels"][panel]["drawn"][role][leg][kind]
        for panel in record.counts["panels"]
        for role in ("base", "compared")
        for leg in pr.LEGS
        for kind in pr.KINDS
    ]
    assert drawn == expected
    rule = spec.rules[0]
    assert record.counts["panels"]["slow_bear_200710"]["in_segment"]["base"] == pr.count_moves(
        pr.leg_moves(rule.legs), "2007-11", "2009-02"
    )


def test_figure_c_ratio_against_the_matched_fixed_exposure(tmp_path: Path) -> None:
    spec = _spec()
    record = pr.figures(spec, tmp_path)[2]
    for rule in spec.rules:
        expected = (1 + rule.returns).prod() / (1 + rule.matched).prod()
        assert record.counts["final_ratio"][rule.role] == pytest.approx(expected, rel=1e-12)
        assert expected != pytest.approx(
            (1 + rule.returns).prod() / (1 + spec.asset_returns).prod()
        )
    root = _root(tmp_path / record.name)
    assert sum(1 for p in root.iter(f"{{{NS['s']}}}polyline") if p.get("class") == "ratio") == 2


def _labels(root: ET.Element) -> dict[str, list[ET.Element]]:
    return {
        g.get("data-id"): g.findall("s:text", NS)
        for g in root.iter(f"{{{NS['s']}}}g")
        if g.get("class") == "segment-label"
    }


def _band_centres(root: ET.Element, area: tuple[float, float, float, float]) -> dict[str, float]:
    ax, ay, aw, ah = area
    return {
        r.get("data-id"): float(r.get("x")) + float(r.get("width")) / 2
        for r in root.iter(f"{{{NS['s']}}}rect")
        if r.get("class") == "band" and float(r.get("y")) == ay
    }


@pytest.mark.parametrize("part", [0, 2])
def test_segment_names_sit_above_the_top_panel_centred_on_their_bands(
    tmp_path: Path, part: int
) -> None:
    record = pr.figures(_spec(), tmp_path)[part]
    root = _root(tmp_path / record.name)
    top = _areas(root)[0]
    labels = _labels(root)
    assert set(labels) == {"slow_bear_200003", "slow_bear_200710", "long_bull_200210"}
    centres = _band_centres(root, top)
    for sid, texts in labels.items():
        assert texts[0].text == {
            "slow_bear_200003": "慢熊 2000-04",
            "slow_bear_200710": "慢熊 2007-11",
        }.get(sid, "长牛 2002-10")
        for text in texts:
            assert float(text.get("y")) < top[1]
            assert float(text.get("x")) == pytest.approx(centres[sid], abs=0.15)


def test_figure_c_labels_carry_each_rules_number(tmp_path: Path) -> None:
    record = pr.figures(_spec(), tmp_path)[2]
    labels = _labels(_root(tmp_path / record.name))
    values = {sid: [(t.text, t.get("fill")) for t in texts[1:]] for sid, texts in labels.items()}
    assert values["slow_bear_200003"] == [("+11.9", "#8c8c8c"), ("+8.0", "#1f77b4")]
    assert values["long_bull_200210"] == [("-3.3/年", "#8c8c8c"), ("-1.2/年", "#1f77b4")]
    assert values["slow_bear_200710"] == [("—", "#8c8c8c"), ("—", "#1f77b4")]
    record_a = pr.figures(_spec("reference"), tmp_path)[0]
    assert all(len(texts) == 1 for texts in _labels(_root(tmp_path / record_a.name)).values())


def test_crowded_labels_move_up_a_tier() -> None:
    panel = pr.Panel(
        0, 200, 600, 100, pd.Timestamp("2000-01-01"), pd.Timestamp("2001-12-31"), 0, 1, False
    )
    bands = [
        pr.SegmentBand("a", "slow_bear", "2000-01", "2000-02"),
        pr.SegmentBand("b", "long_bull", "2000-03", "2000-04"),
        pr.SegmentBand("c", "slow_bear", "2001-06", "2001-07"),
        pr.SegmentBand("d", "fast_drop", "2000-05", "2000-06"),
    ]
    blocks = pr.label_blocks(panel, bands)
    assert [b.id for b in blocks] == ["a", "b", "c"]
    assert pr.label_tiers(blocks) == [0, 1, 0]
    assert pr.band_height(blocks) == 2 * (pr.LABEL_LINE + 4)
    assert pr.band_height([]) == 0.0


def test_bands_and_colours(tmp_path: Path) -> None:
    record = pr.figures(_spec(), tmp_path)[0]
    svg = (tmp_path / record.name).read_text(encoding="utf-8")
    kinds = re.findall(r'class="band" data-kind="([a-z_]+)"', svg)
    assert set(kinds) == {"slow_bear", "long_bull", "fast_drop"}
    assert 'data-rule="base" data-leg="slow" data-kind="exit" stroke="#8c8c8c"' in svg
    assert 'data-rule="compared" data-leg="fast" data-kind="entry" stroke="#1f77b4"' in svg


def test_spec_checks(tmp_path: Path) -> None:
    spec = _spec()
    with pytest.raises(ValueError, match="one or two"):
        pr.figures(
            pr.FigureSpec(**{**spec.__dict__, "rules": [*spec.rules, spec.rules[0]]}), tmp_path
        )
    with pytest.raises(ValueError, match="own role"):
        pr.figures(
            pr.FigureSpec(**{**spec.__dict__, "rules": [spec.rules[0], spec.rules[0]]}), tmp_path
        )
    rule = spec.rules[0]
    short = pr.RuleSeries(
        "x", "base", rule.exposure.iloc[1:], rule.returns, rule.legs, rule.matched
    )
    with pytest.raises(ValueError, match="return dates"):
        pr.figures(pr.FigureSpec(**{**spec.__dict__, "rules": [short]}), tmp_path)
    unmatched = pr.RuleSeries(
        "x", "base", rule.exposure, rule.returns, rule.legs, rule.matched.iloc[1:]
    )
    with pytest.raises(ValueError, match="return dates"):
        pr.figures(pr.FigureSpec(**{**spec.__dict__, "rules": [unmatched]}), tmp_path)
    assert not list(tmp_path.iterdir())
