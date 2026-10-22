"""把调用方给定窗口的收益、两腿持有值和段读数画成 SVG。

只处理传入序列，不读市场数据，不选择研究窗口、不判策略或授权任何动作。
A 总览、B 慢熊放大、C 同平均敞口累计比值共用同一套生效日出入计数。
标签、图例和刻度都在绘图区外；输出确定、可校验，不覆盖已有图。
腿是择时的快慢两腿（H-A），或储备腿（H-B，1 = 国债、0 = 现金）；竖线连箭头都在绘图区里。
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from xml.sax.saxutils import escape

import numpy as np
import pandas as pd

Role = Literal["base", "compared"]
LEGS = ("slow", "fast")
ALL_LEGS = (*LEGS, "reserve")
LEG_SETS = (["fast", "slow"], ["reserve"])
LEG_TEXT = {"slow": "慢腿 252 日", "fast": "快腿 21 日", "reserve": "国债腿"}
KINDS = ("exit", "entry")
COLORS = {"base": "#8c8c8c", "compared": "#1f77b4"}
SHADES = {
    "slow_bear": "#cfe2f3",
    "fast_drop": "#fde2c8",
    "long_bull": "#edf7ee",
    "decline": "#e6dff2",
}
SHADE_TEXT = {"slow_bear": "慢熊", "fast_drop": "快跌", "long_bull": "长牛", "decline": "下跌 10%"}
STEM = {
    ("base", "slow"): 0.12,
    ("base", "fast"): 0.07,
    ("compared", "slow"): 0.095,
    ("compared", "fast"): 0.05,
    ("base", "reserve"): 0.12,
    ("compared", "reserve"): 0.095,
}
STEM_WIDTH = {"slow": 1.3, "fast": 0.9, "reserve": 1.3}
LEGEND_TEXT = "#333"
# 有竖线的对数面板上下各留 0.2 倍跨度，最长的竖线连箭头都在绘图区里。
STEM_PAD = 0.2
PAD = 0.12
CLOSE_UP_SCALE = 0.6
BEFORE_MONTHS = 3
AFTER_MONTHS = 6
FONT = "Microsoft YaHei, WenQuanYi Zen Hei, sans-serif"
LABELLED = ("slow_bear", "long_bull", "decline")
LABEL_SIZE = 9.5
LABEL_LINE = 12.0


@dataclass(frozen=True)
class RuleSeries:
    name: str
    role: Role
    exposure: pd.Series
    returns: pd.Series
    legs: pd.DataFrame
    matched: pd.Series


@dataclass(frozen=True)
class SegmentBand:
    id: str
    kind: str
    start: str
    end: str


@dataclass(frozen=True)
class SegmentReading:
    difference: float | None
    exit_lag_days: int | None = None
    reentry_lag_days: int | None = None
    ratio: float | None = None
    held_months: int | None = None
    months: int | None = None
    ratio_name: str = "r"  # 比值在图上叫什么。


@dataclass(frozen=True)
class FigureSpec:
    step: str
    market: str
    title: str
    window_id: str
    asset_returns: pd.Series
    segments: Sequence[SegmentBand]
    rules: Sequence[RuleSeries]
    readings: Mapping[str, Mapping[str, SegmentReading]] = field(default_factory=dict)
    footer: Sequence[str] = ()
    closeups: Sequence[str] = ()


@dataclass(frozen=True)
class FigureRecord:
    name: str
    sha256: str
    bytes: int
    counts: dict[str, Any]


@dataclass(frozen=True)
class LabelBlock:
    id: str
    kind: str
    x: float
    lines: tuple[tuple[str, str], ...]

    @property
    def half(self) -> float:
        return max(text_width(text, LABEL_SIZE) for text, _ in self.lines) / 2 + 3


@dataclass
class Panel:
    x: float
    y: float
    width: float
    height: float
    first: pd.Timestamp
    last: pd.Timestamp
    low: float
    high: float
    log: bool

    def px(self, when: pd.Timestamp) -> float:
        span = (self.last - self.first).days
        return self.x + self.width * (when - self.first).days / (span or 1)

    def py(self, value: float) -> float:
        scaled = math.log10(value) if self.log else value
        return self.y + self.height * (self.high - scaled) / (self.high - self.low)


def fmt(value: float) -> str:
    text = f"{value:.1f}"
    return "0.0" if text == "-0.0" else text


class Svg:
    def __init__(self, width: float, height: float) -> None:
        self.parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{fmt(width)}" '
            f'height="{fmt(height)}" viewBox="0.0 0.0 {fmt(width)} {fmt(height)}" '
            f'font-family="{FONT}">',
            f'<rect x="0.0" y="0.0" width="{fmt(width)}" height="{fmt(height)}" fill="white"/>',
        ]

    def add(self, text: str) -> None:
        self.parts.append(text)

    def text(
        self,
        x: float,
        y: float,
        text: str,
        *,
        size: float = 11,
        anchor: str = "start",
        color: str = "#222",
        weight: str = "normal",
    ) -> None:
        self.add(
            f'<text x="{fmt(x)}" y="{fmt(y)}" font-size="{fmt(size)}" '
            f'text-anchor="{escape(anchor)}" fill="{escape(color)}" '
            f'font-weight="{escape(weight)}">{escape(text)}</text>'
        )

    def render(self) -> bytes:
        return ("\n".join([*self.parts, "</svg>"]) + "\n").encode("utf-8")


def rule_legs(legs: pd.DataFrame) -> tuple[str, ...]:
    return tuple(leg for leg in ALL_LEGS if leg in legs.columns)


def leg_moves(legs: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for leg in rule_legs(legs):
        for when, change in legs[leg].diff().items():
            value = float(change)
            if math.isfinite(value) and value != 0:
                rows.append((when, leg, "exit" if value < 0 else "entry", abs(value)))
    return pd.DataFrame(rows, columns=["date", "leg", "kind", "size"]).sort_values(
        ["date", "leg"], kind="stable", ignore_index=True
    )


def count_moves(
    moves: pd.DataFrame,
    start: str | None = None,
    end: str | None = None,
    legs: Sequence[str] = LEGS,
) -> dict[str, dict[str, int]]:
    months = pd.DatetimeIndex(moves["date"]).to_period("M")
    inside = np.ones(len(moves), dtype=bool)
    if start is not None:
        inside &= months >= pd.Period(start, "M")
    if end is not None:
        inside &= months <= pd.Period(end, "M")
    part = moves.loc[inside]
    return {
        leg: {kind: int(((part["leg"] == leg) & (part["kind"] == kind)).sum()) for kind in KINDS}
        for leg in legs
    }


def text_width(text: str, size: float) -> float:
    return sum(size if ord(char) > 0x2E7F else 0.6 * size for char in text)


def _span(panel: Panel, segment: SegmentBand) -> tuple[pd.Timestamp, pd.Timestamp]:
    return (
        max(panel.first, pd.Period(segment.start, "M").start_time),
        min(panel.last, pd.Period(segment.end, "M").end_time.normalize()),
    )


def label_blocks(
    panel: Panel,
    segments: Sequence[SegmentBand],
    rules: Sequence[RuleSeries] = (),
    readings: Mapping[str, Mapping[str, SegmentReading]] | None = None,
) -> list[LabelBlock]:
    blocks = []
    for segment in segments:
        first, last = _span(panel, segment)
        if segment.kind not in LABELLED or last <= first:
            continue
        lines = [(f"{SHADE_TEXT[segment.kind]} {segment.start}", "#333")]
        for rule in rules:
            reading = (readings or {}).get(segment.id, {}).get(rule.role)
            if segment.kind == "decline":
                text = (
                    "—"
                    if reading is None or reading.ratio is None
                    else f"{reading.ratio_name} {reading.ratio:+.2f}"
                )
            else:
                value = None if reading is None else reading.difference
                text = "—" if value is None else f"{value * 100:+.1f}"
                if value is not None and segment.kind == "long_bull":
                    text += "/年"
            lines.append((text, COLORS[rule.role]))
        blocks.append(
            LabelBlock(
                segment.id, segment.kind, (panel.px(first) + panel.px(last)) / 2, tuple(lines)
            )
        )
    return sorted(blocks, key=lambda block: block.x)


def label_tiers(blocks: Sequence[LabelBlock]) -> list[int]:
    ends: list[float] = []
    tiers = []
    for block in blocks:
        tier = next((i for i, end in enumerate(ends) if block.x - block.half > end), len(ends))
        if tier == len(ends):
            ends.append(block.x + block.half)
        else:
            ends[tier] = block.x + block.half
        tiers.append(tier)
    return tiers


def band_height(blocks: Sequence[LabelBlock]) -> float:
    if not blocks:
        return 0.0
    return (max(label_tiers(blocks)) + 1) * (max(len(b.lines) for b in blocks) * LABEL_LINE + 4)


def _labels(svg: Svg, panel: Panel, blocks: Sequence[LabelBlock]) -> None:
    if not blocks:
        return
    height = max(len(b.lines) for b in blocks) * LABEL_LINE + 4
    for block, tier in zip(blocks, label_tiers(blocks), strict=True):
        svg.add(
            f'<g class="segment-label" data-id="{escape(block.id)}" '
            f'data-kind="{escape(block.kind)}">'
        )
        for i, (text, color) in enumerate(block.lines):
            svg.text(
                block.x,
                panel.y - 4 - tier * height - (len(block.lines) - i - 1) * LABEL_LINE,
                text,
                size=LABEL_SIZE,
                anchor="middle",
                color=color,
            )
        svg.add("</g>")


def plot_area(svg: Svg, panel: Panel) -> None:
    svg.add(
        f'<rect class="plot-area" x="{fmt(panel.x)}" y="{fmt(panel.y)}" '
        f'width="{fmt(panel.width)}" height="{fmt(panel.height)}" '
        'fill="none" stroke="#777"/>'
    )


def log_bounds(
    values: Sequence[float] | np.ndarray[Any, Any] | pd.Series,
    pad: float = PAD,
) -> tuple[float, float]:
    valid = [math.log10(float(v)) for v in values if math.isfinite(float(v)) and v > 0]
    if not valid:
        raise ValueError("a log panel needs positive finite values")
    low, high = min(valid), max(valid)
    padding = max(high - low, 0.1) * pad
    return low - padding, high + padding


def y_ticks(svg: Svg, panel: Panel) -> None:
    if panel.log:
        ticks = [
            m * 10.0**k
            for k in range(math.floor(panel.low), math.ceil(panel.high) + 1)
            for m in (1, 2, 5)
            if panel.low <= math.log10(m * 10.0**k) <= panel.high
        ]
        if len(ticks) < 4:
            ticks = [10.0**v for v in np.linspace(panel.low, panel.high, 5)]
    else:
        ticks = list(np.linspace(panel.low, panel.high, 5))
    for value in ticks:
        y = panel.py(value)
        svg.add(
            f'<line x1="{fmt(panel.x)}" y1="{fmt(y)}" '
            f'x2="{fmt(panel.x + panel.width)}" y2="{fmt(y)}" stroke="#ddd"/>'
        )
        svg.text(panel.x - 8, y + 3, f"{value:.3g}", size=10, anchor="end")


def _polyline(
    svg: Svg, panel: Panel, series: pd.Series, color: str, width: float, cls: str, step: bool
) -> None:
    points: list[tuple[float, float]] = []

    def flush() -> None:
        if points:
            coords = " ".join(f"{fmt(x)},{fmt(y)}" for x, y in points)
            svg.add(
                f'<polyline class="{cls}" points="{coords}" fill="none" '
                f'stroke="{color}" stroke-width="{fmt(width)}"/>'
            )
            points.clear()

    for date, raw in zip(pd.DatetimeIndex(series.index), series.to_numpy(), strict=True):
        if date < panel.first or date > panel.last:
            continue
        value = float(raw)
        if not math.isfinite(value) or (panel.log and value <= 0):
            flush()
            continue
        x, y = panel.px(date), panel.py(value)
        if step and points:
            points.append((x, points[-1][1]))
        points.append((x, y))
    flush()


def line(svg: Svg, panel: Panel, series: pd.Series, color: str, width: float, cls: str) -> None:
    _polyline(svg, panel, series, color, width, cls, False)


def step_line(svg: Svg, panel: Panel, series: pd.Series, color: str, cls: str) -> None:
    _polyline(svg, panel, series, color, 1.3, cls, True)


def legend(svg: Svg, x: float, y: float, items: Sequence[tuple[str, str, float, str]]) -> None:
    for text, color, width, mark in items:
        if mark == "band":
            svg.add(
                f'<rect x="{fmt(x)}" y="{fmt(y - 9)}" width="16.0" height="9.0" fill="{color}"/>'
            )
        elif mark in ("line", "stem"):
            svg.add(
                f'<line x1="{fmt(x)}" y1="{fmt(y - 4)}" x2="{fmt(x + 16)}" '
                f'y2="{fmt(y - 4)}" stroke="{color}" stroke-width="{fmt(width)}"/>'
            )
        svg.text(x + 21, y, text, color=LEGEND_TEXT if mark == "band" else color)
        x += 21 + text_width(text, 11) + 20


def _bands(svg: Svg, panel: Panel, segments: Sequence[SegmentBand]) -> None:
    for segment in segments:
        first, last = _span(panel, segment)
        if last <= first:
            continue
        svg.add(
            f'<rect class="band" data-kind="{escape(segment.kind)}" '
            f'data-id="{escape(segment.id)}" x="{fmt(panel.px(first))}" '
            f'y="{fmt(panel.y)}" width="{fmt(panel.px(last) - panel.px(first))}" '
            f'height="{fmt(panel.height)}" fill="{SHADES[segment.kind]}" stroke="none"/>'
        )


def _stems(
    svg: Svg, panel: Panel, spec: FigureSpec, price: pd.Series, scale: float
) -> dict[str, Any]:
    drawn: dict[str, Any] = {}
    for rule in spec.rules:
        moves = leg_moves(rule.legs)
        dates = pd.DatetimeIndex(moves["date"])
        moves = moves.loc[(dates >= panel.first) & (dates <= panel.last)]
        drawn[rule.role] = count_moves(moves, legs=rule_legs(rule.legs))
        for leg in rule_legs(rule.legs):
            for kind in KINDS:
                color = COLORS[rule.role]
                svg.add(
                    f'<g class="stems" data-rule="{rule.role}" data-leg="{leg}" '
                    f'data-kind="{kind}" stroke="{color}" fill="{color}">'
                )
                for move in moves.loc[(moves["leg"] == leg) & (moves["kind"] == kind)].itertuples():
                    when = pd.Timestamp(str(move.date))
                    value = float(price.loc[when])
                    if not math.isfinite(value) or value <= 0:
                        raise ValueError("a stem needs a positive finite asset level")
                    x = panel.px(when)
                    y1 = float(fmt(panel.py(value)))
                    length = STEM[(rule.role, leg)] * scale * panel.height
                    y2 = y1 + (-length if kind == "exit" else length)
                    svg.add(
                        f'<line x1="{fmt(x)}" y1="{fmt(y1)}" x2="{fmt(x)}" '
                        f'y2="{fmt(y2)}" stroke-width="{fmt(STEM_WIDTH[leg])}"/>'
                    )
                    back = y2 - 4 if kind == "exit" else y2 + 4
                    svg.add(
                        f'<polygon points="{fmt(x)},{fmt(y2)} '
                        f'{fmt(x - 3)},{fmt(back)} {fmt(x + 3)},{fmt(back)}"/>'
                    )
                svg.add("</g>")
    return drawn


def _years(svg: Svg, panel: Panel) -> None:
    for year in range(panel.first.year, panel.last.year + 1):
        when = max(panel.first, pd.Timestamp(year=year, month=1, day=1))
        svg.text(panel.px(when), panel.y + panel.height + 17, str(year), size=10, anchor="middle")


def _move_text(leg: str, counts: Mapping[str, int]) -> str:
    if leg == "reserve":
        return f"转国债 {counts['entry']} 次，转现金 {counts['exit']} 次"
    return f"出 {counts['exit']} 入 {counts['entry']}"


def _common_legend(svg: Svg, spec: FigureSpec) -> None:
    legend(
        svg,
        80,
        55,
        [
            (SHADE_TEXT[k] + ("（只上底色，不标名）" if k == "fast_drop" else ""), color, 1, "band")
            for k, color in SHADES.items()
            if (k == "decline") == any(s.kind == "decline" for s in spec.segments)
        ],
    )
    for i, rule in enumerate(spec.rules):
        counts = count_moves(leg_moves(rule.legs), legs=rule_legs(rule.legs))
        legend(
            svg,
            80,
            77 + 22 * i,
            [
                (
                    f"{rule.name} {LEG_TEXT[leg]}：{_move_text(leg, counts[leg])}",
                    COLORS[rule.role],
                    STEM_WIDTH[leg],
                    "stem",
                )
                for leg in rule_legs(rule.legs)
            ],
        )
    svg.text(
        80,
        125,
        "竖线朝下 ▲ 转持国债，朝上 ▼ 转持现金；落在生效日（t+2）的价格上"
        if any("reserve" in r.legs for r in spec.rules)
        else "竖线朝上 ▼ 出场，朝下 ▲ 入场；落在生效日（t+2）的价格上",
    )


def _footer(svg: Svg, spec: FigureSpec, y: float) -> None:
    for i, text in enumerate(spec.footer):
        svg.text(80, y + 18 * i, text, size=10)


def _exposure(svg: Svg, panel: Panel, spec: FigureSpec, mean: bool) -> None:
    _bands(svg, panel, spec.segments)
    y_ticks(svg, panel)
    items = []
    for rule in spec.rules:
        color = COLORS[rule.role]
        step_line(svg, panel, rule.exposure, color, "exposure")
        items.append((f"{rule.name} 持有敞口", color, 1.3, "line"))
        if "reserve" in rule.legs and rule.legs["reserve"].dropna().nunique() > 1:
            step_line(svg, panel, rule.legs["reserve"], color, "reserve")
            items.append((f"{rule.name} 国债腿（1 = 国债，0 = 现金）", color, 1.3, "line"))
        if mean:
            average = float(rule.exposure.mean())
            y = panel.py(average)
            svg.add(
                f'<line class="mean" x1="{fmt(panel.x)}" y1="{fmt(y)}" '
                f'x2="{fmt(panel.x + panel.width)}" y2="{fmt(y)}" '
                f'stroke="{color}" stroke-dasharray="5 4"/>'
            )
            items.append((f"{rule.name} 平均敞口 {average:.2f}", color, 1, "line"))
    legend(svg, panel.x, panel.y - 14, items)
    _years(svg, panel)
    plot_area(svg, panel)


def _overview(spec: FigureSpec, ratio: bool) -> tuple[bytes, dict[str, Any]]:
    price = (1 + spec.asset_returns).cumprod()
    dates = pd.DatetimeIndex(price.index)
    ratios = {r.role: (1 + r.returns).cumprod() / (1 + r.matched).cumprod() for r in spec.rules}
    values = np.concatenate([s.to_numpy(dtype=float) for s in ratios.values()]) if ratio else price
    low, high = log_bounds(values, PAD if ratio else STEM_PAD)
    width = max(1400, 30 * (dates[-1].year - dates[0].year + 1)) + 140
    top = Panel(80, 0, width - 140, 420 if ratio else 520, dates[0], dates[-1], low, high, True)
    blocks = label_blocks(top, spec.segments, spec.rules if ratio else (), spec.readings)
    top.y = (192 if ratio else 146) + band_height(blocks)
    bottom = Panel(
        80, top.y + top.height + 70, top.width, 170, dates[0], dates[-1], -0.05, 1.08, False
    )
    foot = bottom.y + bottom.height + 50
    svg = Svg(width, foot + len(spec.footer) * 18 + 20)
    heading = (
        f"图 C 代价和收益：{spec.title}（规则累计净值 / 同平均敞口固定敞口的累计，对数轴）"
        if ratio
        else f"图 A 总览：{spec.title}"
    )
    svg.text(80, 28, heading, size=16, weight="bold")
    _common_legend(svg, spec)
    if ratio:
        legend(
            svg,
            80,
            148,
            [(f"{r.name}：累计比值", COLORS[r.role], 1.5, "line") for r in spec.rules]
            + [("1 = 和同平均敞口的固定敞口一样", "#555", 1, "line")],
        )
        svg.text(
            80,
            170,
            "段上方：段名和每条规则的数（灰 = 基准规则，蓝 = 对比规则）；"
            "慢熊写 D（点），长牛写年化差（点/年），都是对同平均敞口固定敞口。",
        )
    _labels(svg, top, blocks)
    _bands(svg, top, spec.segments)
    y_ticks(svg, top)
    if ratio:
        y = top.py(1)
        svg.add(
            f'<line class="parity" x1="{fmt(top.x)}" y1="{fmt(y)}" '
            f'x2="{fmt(top.x + top.width)}" y2="{fmt(y)}" '
            'stroke="#777" stroke-dasharray="5 4"/>'
        )
        for rule in spec.rules:
            line(svg, top, ratios[rule.role], COLORS[rule.role], 1.5, "ratio")
        counts: dict[str, Any] = {"final_ratio": {r: float(s.iloc[-1]) for r, s in ratios.items()}}
    else:
        line(svg, top, price, "#222", 1.2, "price")
        counts = {
            "window": {
                r.role: count_moves(leg_moves(r.legs), legs=rule_legs(r.legs)) for r in spec.rules
            },
            "drawn": _stems(svg, top, spec, price, 1),
        }
    _years(svg, top)
    plot_area(svg, top)
    _exposure(svg, bottom, spec, not ratio)
    _footer(svg, spec, foot)
    return svg.render(), counts


def _closeups(spec: FigureSpec) -> tuple[bytes, dict[str, Any]]:
    if spec.closeups:
        by_id = {s.id: s for s in spec.segments}
        for sid in spec.closeups:
            if sid not in by_id:
                raise ValueError(f"figure B has no segment {sid}")
        bears = [by_id[sid] for sid in spec.closeups]
    else:
        bears = [s for s in spec.segments if s.kind == "slow_bear"]
    if not bears:
        raise ValueError("figure B needs a slow bear segment")
    price = (1 + spec.asset_returns).cumprod()
    dates = pd.DatetimeIndex(price.index)
    cols = min(5, len(bears))
    rows = math.ceil(len(bears) / cols)
    foot = 150 + rows * 430 + 20
    svg = Svg(max(1540, 80 + cols * 600), foot + len(spec.footer) * 18 + 20)
    kinds = "、".join(dict.fromkeys(SHADE_TEXT[s.kind] for s in bears))
    svg.text(80, 28, f"图 B {kinds}段放大：{spec.title}", size=16, weight="bold")
    _common_legend(svg, spec)
    panels = {}
    for i, bear in enumerate(bears):
        first = max(dates[0], (pd.Period(bear.start, "M") - BEFORE_MONTHS).start_time)
        last = min(dates[-1], (pd.Period(bear.end, "M") + AFTER_MONTHS).end_time.normalize())
        inside = price.loc[(price.index >= first) & (price.index <= last)]
        low, high = log_bounds(inside, STEM_PAD)
        x, y = 80 + (i % cols) * 600, 150 + (i // cols) * 430
        panel = Panel(x, y + 80, 500, 300, first, last, low, high, True)
        svg.text(x, y, f"{bear.id}（{bear.start}..{bear.end}）", weight="bold")
        for j, rule in enumerate(spec.rules):
            reading = spec.readings.get(bear.id, {}).get(rule.role)
            if reading is None:
                text = "—"
            elif bear.kind == "decline":
                r = "—" if reading.ratio is None else f"{reading.ratio:+.3f}"
                held = "—" if reading.held_months is None else str(reading.held_months)
                months = "—" if reading.months is None else str(reading.months)
                text = f"{reading.ratio_name} {r}，段内持国债 {held}/{months} 月"
            else:
                d = "—" if reading.difference is None else f"{reading.difference * 100:+.1f} 点"
                n = "—" if reading.exit_lag_days is None else f"{reading.exit_lag_days} 天"
                m = "—" if reading.reentry_lag_days is None else f"{reading.reentry_lag_days} 天"
                text = f"D {d}，出场滞后 {n}，回仓滞后 {m}"
            svg.text(x, y + 22 * (j + 1), f"{rule.name}：{text}", color=COLORS[rule.role])
        _bands(svg, panel, spec.segments)
        y_ticks(svg, panel)
        line(svg, panel, price, "#222", 1.2, "price")
        panels[bear.id] = {
            "drawn": _stems(svg, panel, spec, price, CLOSE_UP_SCALE),
            "in_segment": {
                r.role: count_moves(leg_moves(r.legs), bear.start, bear.end, rule_legs(r.legs))
                for r in spec.rules
            },
        }
        _years(svg, panel)
        plot_area(svg, panel)
    _footer(svg, spec, foot)
    return svg.render(), {"panels": panels}


def figures(spec: FigureSpec, out_dir: Path) -> list[FigureRecord]:
    if not 1 <= len(spec.rules) <= 2:
        raise ValueError("a figure compares one or two rules")
    if len({r.role for r in spec.rules}) != len(spec.rules):
        raise ValueError("each rule needs its own role")
    for rule in spec.rules:
        if any(
            not s.index.equals(spec.asset_returns.index)
            for s in (rule.exposure, rule.returns, rule.matched)
        ):
            raise ValueError(f"{rule.name}: series must be on the asset's return dates")
        if (
            not rule.legs.index.equals(spec.asset_returns.index)
            or list(rule.legs.columns) not in LEG_SETS
        ):
            raise ValueError(
                f"{rule.name}: legs need fast and slow, or reserve, on the asset's return dates"
            )
    names = [f"{spec.step}-{spec.market}-{part}.svg" for part in "ABC"]
    for name in names:
        if (out_dir / name).exists():
            raise FileExistsError(out_dir / name)
    # Render before opening files, so an unusable panel cannot leave a partial figure set.
    rendered = [_overview(spec, False), _closeups(spec), _overview(spec, True)]
    out_dir.mkdir(parents=True, exist_ok=True)
    records = []
    for name, (data, counts) in zip(names, rendered, strict=True):
        with (out_dir / name).open("xb") as handle:
            handle.write(data)
        records.append(FigureRecord(name, hashlib.sha256(data).hexdigest(), len(data), counts))
    return records
