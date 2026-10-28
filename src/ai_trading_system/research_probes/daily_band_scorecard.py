"""日报仓位成绩单：依据 owner_decision:TRADING-2565:2026-10-28:daily_band_scorecard。

第 6 期章程第 21 节：只读日报状态目录里已经写下的决策快照和一个累计价格捕获文件，
按目录的先后取每天的快照；不写日报目录、不改日报代码和配置、不写前瞻账本，
forward_ledger 只导入。持有照前瞻账本的 t+2；六条线和同平均敞口的固定线，
现金按 0、另印一组按 SGOV；全窗口和去掉陈旧天两遍汇总，比较在交集天上。
事件和拆因照快照原样，不重算评分。描述，不判，不计入 F0008、前瞻核对或正式题计数，
起点以前的价格只用于 F0001 的预热；甲、乙只机械算条件。
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import typer

from ai_trading_system.config import (
    DEFAULT_SCORING_RULES_CONFIG_PATH,
    PROJECT_ROOT,
    load_scoring_rules,
)
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_probes import forward_ledger as fl
from ai_trading_system.trading_calendar import is_us_equity_trading_day

RUN_ID = "TRADING-2565-80"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-28:daily_band_scorecard"
SNAPSHOT_DIR = "data/processed/decision_snapshots"
CAPTURE_DIR = "data/raw/daily_input_capture"
PRICES_NAME = "market_macro/prices_daily.csv"
SNAPSHOT_NAME = re.compile(r"decision_snapshot_(\d{4}-\d{2}-\d{2})\.json")
FINAL = "final"
LINES = (FINAL, "total_asset", "model", "fixed", "f0001", "total_risk")
OWN_FIXED = (FINAL, "total_asset", "model", "f0001")
BAND_FIELDS = {
    FINAL: "final_risk_asset_ai_band",
    "total_asset": "total_asset_ai_band",
    "model": "model_risk_asset_ai_band",
    "total_risk": "final_total_risk_asset_band",
}
FIELDS = (
    "model_risk_asset_ai_band",
    "total_asset_ai_band",
    "final_total_risk_asset_band",
    "macro_risk_asset_budget.level",
    "position_gates",
    "overall_score",
    "confidence_score",
    "confidence_level",
    "confidence_reasons",
    "components",
    "rule_versions.rules",
)
# 持有快照比当天早这么多个交易日及以上算陈旧（正常是 2）；描述用的标记，不是研究阈值。
STALE_LAG = 5
# 连续这么多个交易日及以上没有可用快照算断档（需求第 3.5 节）。
GAP_DAYS = 3
# 第 21 节要求的事件描述窗口，按生效日起算，不是研究阈值。
HORIZONS = (5, 20)
# 需求第 5.1 节跑之前写死的读法：甲的线（对数），乙的次数和比例（分子、分母）。
LINE_A, EVENTS_B, SHARE_B = -0.01, 6, (2, 3)
# 浮点比较的容差（恒等核对、闸门上限、分档映射、换仓步长），不是研究阈值。
EPSILON = 1e-9
SUMMARY, REPORT = "daily_band_scorecard.json", "daily_band_scorecard.md"
SCORE_MODEL = "score_model"
NO_EXTRA = "无额外闸门"
NO_MATCH = "没有对上的闸门"
MISSING = "缺"
NOT_A_READ = "描述，不判；不是一致性窗口或验证窗口的读取。"
LABELS = {
    FINAL: "日报·风险资产内 AI 区间中点",
    "total_asset": "日报·总资产 AI 区间中点",
    "model": "日报·模型区间中点（闸门前）",
    "fixed": "固定 0.8",
    "f0001": "F0001 覆盖",
    "total_risk": "日报·股票/风险资产预算中点（只印）",
}
ASSUMPTIONS = {
    FINAL: "把总风险资产预算当作 100%，是前瞻账本的口径。",
    "total_asset": "非 AI 股票和现金都按现金算，所以偏低。",
    "model": "闸门前的模型区间，和第 1 条同一种口径，用来看闸门本身花了多少。",
    "fixed": "总资产里 QQQ 占 0.8，其余现金。",
    "f0001": "QQQ-Q3 的 fast_slow，和前瞻账本同一个函数，成本 0。",
    "total_risk": "假设非 AI 股票和 QQQ 同涨同跌，所以偏高；不进甲，"
    "只给「照日报做」的总敞口定个上头。",
}
app = typer.Typer(help="日报仓位成绩单：只读日报的快照和价格，描述，不判。")


class ScorecardError(ValueError):
    """缺了就不能跑的输入；什么都不写。"""


@dataclass(frozen=True)
class Bounds:
    low: float
    high: float

    @property
    def mid(self) -> float:
        return (self.low + self.high) / 2


@dataclass(frozen=True)
class Gate:
    gate_id: str
    max_position: float | None
    triggered: bool | None
    reason: str | None


@dataclass(frozen=True)
class Component:
    name: str
    score: float
    weight: float


@dataclass(frozen=True)
class Snapshot:
    day: date
    root: int
    file: str
    bands: Mapping[str, Bounds | None]
    macro_level: str | None
    gates: tuple[Gate, ...] | None
    overall_score: float | None
    confidence_score: float | None
    confidence_level: str | None
    confidence_reasons: tuple[str, ...] | None
    components: tuple[Component, ...] | None
    rules: tuple[tuple[str, str], ...] | None
    missing: tuple[str, ...]


@dataclass(frozen=True)
class Selection:
    snapshots: dict[date, Snapshot]
    unusable: list[dict[str, Any]]
    overlaps: list[dict[str, Any]]
    roots: list[dict[str, Any]]


@dataclass(frozen=True)
class Prices:
    file: str
    sha256: str
    qqq: pd.Series
    cash: pd.Series


def _number(value: Any) -> float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    try:
        return float(value) if math.isfinite(value) else None
    except OverflowError:
        return None


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _bounds(value: Any) -> Bounds | None:
    if not isinstance(value, dict):
        return None
    low, high = _number(value.get("min_position")), _number(value.get("max_position"))
    if low is None or high is None or not 0 <= low <= high <= 1:
        return None
    return Bounds(low, high)


def _pair(band: Bounds | None) -> list[float] | None:
    return None if band is None else [band.low, band.high]


def _same(left: Bounds, right: Bounds) -> bool:
    return abs(left.low - right.low) <= EPSILON and abs(left.high - right.high) <= EPSILON


def parse_snapshot(text: str, day: date, root: int, file: str) -> Snapshot:
    try:
        record = json.loads(text)
    except ValueError:
        raise ValueError("invalid JSON") from None
    if not isinstance(record, dict):
        raise ValueError("snapshot is not a dict")
    positions = record.get("positions")
    if not isinstance(positions, dict):
        raise ValueError("positions is not a dict")
    bands = {key: _bounds(positions.get(field)) for key, field in BAND_FIELDS.items()}
    if bands[FINAL] is None:
        raise ValueError(
            "final_risk_asset_ai_band: missing or invalid min_position or max_position"
        )
    scores = _dict(record.get("scores"))
    macro = _string(_dict(positions.get("macro_risk_asset_budget")).get("level"))
    raw_gates = positions.get("position_gates")
    gates = None
    if isinstance(raw_gates, list):
        gates = tuple(
            Gate(
                item["gate_id"],
                _number(item.get("max_position")),
                item.get("triggered") if isinstance(item.get("triggered"), bool) else None,
                _string(item.get("reason")),
            )
            for item in raw_gates
            if isinstance(item, dict) and isinstance(item.get("gate_id"), str)
        )
    raw_components = scores.get("components")
    components = None
    if isinstance(raw_components, list):
        parsed = []
        for item in raw_components:
            if not isinstance(item, dict) or not isinstance(item.get("component"), str):
                continue
            score, weight = _number(item.get("score")), _number(item.get("weight"))
            if score is not None and weight is not None:
                parsed.append(Component(item["component"], score, weight))
        components = tuple(parsed)
    reasons = scores.get("confidence_reasons")
    confidence_reasons = (
        tuple(reasons)
        if isinstance(reasons, list) and all(isinstance(reason, str) for reason in reasons)
        else None
    )
    raw_rules = _dict(record.get("rule_versions")).get("rules")
    rules = (
        tuple(
            (item["rule_id"], str(item.get("version")))
            for item in raw_rules
            if isinstance(item, dict) and isinstance(item.get("rule_id"), str)
        )
        if isinstance(raw_rules, list)
        else None
    )
    overall = _number(scores.get("overall_score"))
    confidence = _number(scores.get("confidence_score"))
    level = _string(scores.get("confidence_level"))
    values: dict[str, Any] = {field: bands[key] for key, field in BAND_FIELDS.items()}
    values.update(
        {
            "macro_risk_asset_budget.level": macro,
            "position_gates": gates,
            "overall_score": overall,
            "confidence_score": confidence,
            "confidence_level": level,
            "confidence_reasons": confidence_reasons,
            "components": components,
            "rule_versions.rules": rules,
        }
    )
    return Snapshot(
        day,
        root,
        file,
        bands,
        macro,
        gates,
        overall,
        confidence,
        level,
        confidence_reasons,
        components,
        rules,
        tuple(field for field in FIELDS if values[field] is None),
    )


def snapshot_files(daily_root: Path) -> tuple[dict[date, str], list[str]]:
    if not daily_root.is_dir():
        raise ScorecardError(f"{daily_root}: no such directory")
    folder = daily_root / SNAPSHOT_DIR
    if not folder.is_dir():
        return {}, []
    files: dict[date, str] = {}
    other = []
    for path in sorted(folder.iterdir()):
        match = SNAPSHOT_NAME.fullmatch(path.name)
        if match is None or not path.is_file():
            continue
        try:
            day = date.fromisoformat(match[1])
        except ValueError:
            other.append(path.name)
            continue
        if is_us_equity_trading_day(day):
            files[day] = f"{SNAPSHOT_DIR}/{path.name}"
        else:
            other.append(path.name)
    return files, other


def read_snapshot(daily_root: Path, root: int, day: date, file: str) -> Snapshot:
    try:
        text = (daily_root / file).read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"cannot read: {exc}") from exc
    return parse_snapshot(text, day, root, file)


def select(daily_roots: Sequence[Path]) -> Selection:
    catalogs = [snapshot_files(root) for root in daily_roots]
    roots: list[dict[str, Any]] = [
        {"path": str(root), "files": len(files), "used": 0, "other_days": other}
        for root, (files, other) in zip(daily_roots, catalogs, strict=True)
    ]
    snapshots: dict[date, Snapshot] = {}
    unusable: list[dict[str, Any]] = []
    overlaps: list[dict[str, Any]] = []
    days = sorted({day for files, _ in catalogs for day in files})
    for day in days:
        candidates = [i for i, (files, _) in enumerate(catalogs) if day in files]
        first = candidates[0]
        parsed: list[Snapshot | str] = []
        for i in candidates:
            try:
                parsed.append(read_snapshot(daily_roots[i], i, day, catalogs[i][0][day]))
            except ValueError as exc:
                parsed.append(str(exc))
        chosen = parsed[0]
        if isinstance(chosen, Snapshot):
            snapshots[day] = chosen
            roots[first]["used"] += 1
        else:
            unusable.append({"day": day.isoformat(), "root": first, "reason": chosen})
        for i, other in zip(candidates[1:], parsed[1:], strict=True):
            left = chosen.bands[FINAL] if isinstance(chosen, Snapshot) else None
            right = other.bands[FINAL] if isinstance(other, Snapshot) else None
            overlaps.append(
                {
                    "day": day.isoformat(),
                    "roots": [first, i],
                    "bands": [
                        _pair(left) if isinstance(chosen, Snapshot) else chosen,
                        _pair(right) if isinstance(other, Snapshot) else other,
                    ],
                    "differ": left is not None and right is not None and not _same(left, right),
                }
            )
    return Selection(snapshots, unusable, overlaps, roots)


def find_prices(daily_root: Path) -> str:
    folder = daily_root / CAPTURE_DIR
    days = []
    if folder.is_dir():
        for path in folder.iterdir():
            if not path.is_dir() or not (path / PRICES_NAME).is_file():
                continue
            try:
                day = date.fromisoformat(path.name)
            except ValueError:
                continue
            if day.isoformat() == path.name:
                days.append(day)
    if not days:
        raise ScorecardError(f"{daily_root}: no {CAPTURE_DIR}/<day>/{PRICES_NAME}")
    return fl.PRICES_REL.format(day=max(days).isoformat())


def load_prices(daily_root: Path) -> Prices:
    relative = find_prices(daily_root)
    try:
        qqq = fl.read_capture(daily_root, relative)
        cash = fl.read_capture(daily_root, relative, fl.CASH_TICKER)
        digest = hashlib.sha256((daily_root / relative).read_bytes()).hexdigest()
    except (fl.ForwardLedgerError, OSError) as exc:
        raise ScorecardError(str(exc)) from exc
    if qqq is None or qqq.empty:
        raise ScorecardError(f"{relative}: no {fl.TICKER} prices")
    return Prices(relative, digest, qqq, pd.Series(dtype=float) if cash is None else cash)


def held_days(days: Sequence[date], snapshots: Mapping[date, Snapshot]) -> list[int | None]:
    held: list[int | None] = []
    latest = None
    for i in range(len(days)):
        if i >= 2 and days[i - 2] in snapshots:
            latest = i - 2
        held.append(latest)
    return held


def stale_days(held: Sequence[int | None]) -> list[bool]:
    return [source is not None and i - source >= STALE_LAG for i, source in enumerate(held)]


def gaps(days: Sequence[date], snapshots: Mapping[date, Snapshot]) -> list[dict[str, Any]]:
    result = []
    first = 0
    for i in range(len(days) + 1):
        if i < len(days) and days[i] not in snapshots:
            continue
        if i - first >= GAP_DAYS:
            result.append(
                {
                    "first": days[first].isoformat(),
                    "last": days[i - 1].isoformat(),
                    "days": i - first,
                }
            )
        first = i + 1
    return result


def exposures(
    days: Sequence[date],
    held: Sequence[int | None],
    snapshots: Mapping[date, Snapshot],
    overlay: pd.Series | None,
) -> dict[str, list[float | None]]:
    lines: dict[str, list[float | None]] = {line: [] for line in LINES}
    for day, source in zip(days, held, strict=True):
        snapshot = snapshots[days[source]] if source is not None else None
        for line in BAND_FIELDS:
            band = snapshot.bands[line] if snapshot is not None else None
            lines[line].append(band.mid if band is not None else None)
        lines["fixed"].append(fl.FIXED_EXPOSURE if snapshot is not None else None)
        stamp = pd.Timestamp(day)
        lines["f0001"].append(
            _number(overlay.loc[stamp])
            if snapshot is not None and overlay is not None and stamp in overlay.index
            else None
        )
    return lines


def noise_scale(exposure: Sequence[float], returns: Sequence[float]) -> float | None:
    if len(exposure) < 2:
        return None
    return statistics.stdev(exposure) * statistics.stdev(returns) * math.sqrt(len(exposure))


def _mean(values: Sequence[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return statistics.fmean(present) if present else None


def line_stats(
    exposure: Sequence[float | None],
    returns: Sequence[float | None],
    indices: Sequence[int],
    *,
    step: float,
    cash: Sequence[float | None] | None = None,
    own_fixed: bool = True,
) -> dict[str, Any]:
    present: list[tuple[float, float, float]] = []
    for i in indices:
        e, r = exposure[i], returns[i]
        c = 0.0 if cash is None else cash[i]
        if e is not None and r is not None and c is not None:
            present.append((e, r, c))
    changes, turnover = 0, 0.0
    included = set(indices)
    for i in indices:
        if i - 1 not in included:
            continue
        before, after = exposure[i - 1], exposure[i]
        if before is not None and after is not None:
            delta = abs(after - before)
            turnover += delta
            changes += delta >= step - EPSILON
    mean = _mean([e for e, _, _ in present])
    own = sum(math.log1p(e * r + (1 - e) * c) for e, r, c in present)
    fixed = sum(
        math.log1p(fl.FIXED_EXPOSURE * r + (1 - fl.FIXED_EXPOSURE) * c) for _, r, c in present
    )
    wealth, peak, drawdown = 1.0, 1.0, 0.0
    for e, r, c in present:
        wealth *= 1 + e * r + (1 - e) * c
        peak = max(peak, wealth)
        drawdown = min(drawdown, wealth / peak - 1)
    result: dict[str, Any] = {
        "days": len(present),
        "log": own if present else None,
        "versus_fixed": own - fixed if present else None,
        "mean_exposure": mean,
        "max_drawdown": drawdown if present else None,
        "changes": changes,
        "turnover": turnover,
    }
    if own_fixed:
        same = (
            sum(math.log1p(mean * r + (1 - mean) * c) for _, r, c in present)
            if mean is not None
            else None
        )
        difference = own - same if same is not None else None
        noise = noise_scale([e for e, _, _ in present], [r for _, r, _ in present])
        result.update(
            versus_own=difference,
            noise=noise,
            units=(
                difference / noise
                if difference is not None and noise is not None and noise != 0
                else None
            ),
        )
    return result


def pass_stats(
    lines: Mapping[str, Sequence[float | None]],
    returns: Sequence[float | None],
    indices: Sequence[int],
    *,
    step: float,
    cash: Sequence[float | None] | None = None,
) -> dict[str, dict[str, Any]]:
    return {
        line: line_stats(
            lines[line], returns, indices, step=step, cash=cash, own_fixed=line in OWN_FIXED
        )
        for line in LINES
    }


def monthly(
    days: Sequence[date],
    lines: Mapping[str, Sequence[float | None]],
    returns: Sequence[float | None],
    indices: Sequence[int],
) -> list[dict[str, Any]]:
    months: dict[str, list[int]] = {}
    for i in indices:
        months.setdefault(days[i].strftime("%Y-%m"), []).append(i)
    result = []
    for month, selected in months.items():
        row: dict[str, Any] = {"month": month}
        for line in LINES:
            values = [
                math.log1p(e * r)
                for i in selected
                if (e := lines[line][i]) is not None and (r := returns[i]) is not None
            ]
            row[line] = sum(values) if values else None
        for line in (FINAL, "total_asset"):
            row[f"{line}_exposure"] = _mean([lines[line][i] for i in selected])
        result.append(row)
    return result


def identity(snapshots: Mapping[date, Snapshot]) -> dict[str, Any]:
    checked, unchecked = 0, 0
    mismatch = []
    for day, snapshot in sorted(snapshots.items()):
        final, total, budget = (snapshot.bands[key] for key in (FINAL, "total_asset", "total_risk"))
        if final is None or total is None or budget is None:
            unchecked += 1
            continue
        checked += 1
        if not _same(total, Bounds(final.low * budget.low, final.high * budget.high)):
            mismatch.append(day.isoformat())
    return {"checked": checked, "unchecked": unchecked, "mismatch": mismatch}


def forward_return(returns: Sequence[float | None], index: int, horizon: int) -> float | None:
    if index < 0 or index + horizon > len(returns):
        return None
    wealth = 1.0
    for value in returns[index : index + horizon]:
        if value is None:
            return None
        wealth *= 1 + value
    return wealth - 1


def events(
    days: Sequence[date],
    mid: Sequence[float | None],
    held: Sequence[int | None],
    returns: Sequence[float | None],
    *,
    step: float,
) -> list[dict[str, Any]]:
    rows = []
    for i in range(1, len(days)):
        before, after = mid[i - 1], mid[i]
        if before is None or after is None or abs(after - before) < step - EPSILON:
            continue
        previous, current = held[i - 1], held[i]
        rows.append(
            {
                "day": days[i].isoformat(),
                "index": i,
                "kind": "down" if after < before else "up",
                "from": before,
                "to": after,
                "snapshots": [
                    days[k].isoformat() if k is not None else None for k in (previous, current)
                ],
                "after_gap": previous is not None
                and current is not None
                and current - previous > GAP_DAYS,
                **{f"next_{horizon}": forward_return(returns, i, horizon) for horizon in HORIZONS},
            }
        )
    return rows


def event_summary(
    rows: Sequence[Mapping[str, Any]], kind: str, returns: Sequence[float | None], first: int
) -> dict[str, Any]:
    selected = [row for row in rows if row["kind"] == kind]
    counted = [row for row in selected if not row["after_gap"] and row["next_20"] is not None]
    non_overlapping, last = 0, None
    for row in counted:
        if last is None or row["index"] >= last + HORIZONS[1]:
            non_overlapping += 1
            last = row["index"]
    base = [
        value
        for i in range(first, len(returns))
        if (value := forward_return(returns, i, HORIZONS[1])) is not None
    ]
    positive = sum(row["next_20"] > 0 for row in counted)
    medians = {}
    for horizon in HORIZONS:
        values = [row[f"next_{horizon}"] for row in counted if row[f"next_{horizon}"] is not None]
        medians[str(horizon)] = statistics.median(values) if values else None
    return {
        "events": len(selected),
        "after_gap": sum(bool(row["after_gap"]) for row in selected),
        "not_full": sum(not row["after_gap"] and row["next_20"] is None for row in selected),
        "counted": len(counted),
        "positive": positive,
        "share": positive / len(counted) if counted else None,
        "median": medians,
        "non_overlapping": non_overlapping,
        "base_days": len(base),
        "base": sum(value > 0 for value in base) / len(base) if base else None,
    }


def reading(passes: Mapping[str, Any], down: Mapping[str, Any]) -> dict[str, Any]:
    a = {}
    for line in (FINAL, "total_asset"):
        values = [passes[key]["zero"][line]["versus_own"] for key in ("full", "fresh")]
        a[line] = all(value is not None and value <= LINE_A for value in values)
    a["holds"] = any(a.values())
    share, base = down["share"], down["base"]
    return {
        "a": a,
        "b": {
            "holds": down["counted"] >= EVENTS_B
            and down["positive"] * SHARE_B[1] >= down["counted"] * SHARE_B[0],
            "above_base": share > base if share is not None and base is not None else None,
        },
    }


def boundaries(bands: Sequence[Any]) -> list[float]:
    return sorted(float(band.min_score) for band in bands if band.min_score > 0)


def mapped(score: float, bands: Sequence[Any]) -> Bounds:
    for band in bands:
        if score >= band.min_score:
            return Bounds(float(band.min_position), float(band.max_position))
    raise ValueError("position bands have no floor")


def binding(snapshot: Snapshot) -> list[str] | None:
    final = snapshot.bands[FINAL]
    if snapshot.gates is None or final is None:
        return None
    return [
        gate.gate_id
        for gate in snapshot.gates
        if gate.triggered is True
        and gate.max_position is not None
        and abs(gate.max_position - final.high) <= EPSILON
    ]


def binding_label(ids: Sequence[str] | None) -> str:
    if ids is None:
        return MISSING
    if not ids:
        return NO_MATCH
    extra = [gate_id for gate_id in ids if gate_id != SCORE_MODEL]
    return "、".join(extra) if extra else NO_EXTRA


def attribution_rows(
    snapshots: Mapping[date, Snapshot], bands: Sequence[Any]
) -> list[dict[str, Any]]:
    edges = boundaries(bands)
    rows = []
    for day, snapshot in sorted(snapshots.items()):
        ids = binding(snapshot)
        gates = [gate for gate in snapshot.gates or () if ids is not None and gate.gate_id in ids]
        score = snapshot.overall_score
        edge = (
            min(edges, key=lambda value: (abs(score - value), value))
            if score is not None and edges
            else None
        )
        model = snapshot.bands["model"]
        mapping = (
            MISSING
            if score is None or model is None
            else ("符合" if _same(mapped(score, bands), model) else "映射不符")
        )
        rows.append(
            {
                "day": day.isoformat(),
                "root": snapshot.root,
                "model": _pair(model),
                "binding": ids,
                "label": binding_label(ids),
                "caps": {gate.gate_id: gate.max_position for gate in gates},
                "reasons": {gate.gate_id: gate.reason for gate in gates},
                "final": _pair(snapshot.bands[FINAL]),
                "budget": _pair(snapshot.bands["total_risk"]),
                "macro_level": snapshot.macro_level,
                "score": score,
                "boundary": edge,
                "distance": score - edge if score is not None and edge is not None else None,
                "mapping": mapping,
            }
        )
    return rows


def versions(ordered: Sequence[Snapshot]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    previous_gates: list[str] | None = None
    previous_rules: list[str] | None = None
    for snapshot in ordered:
        gates = (
            sorted({gate.gate_id for gate in snapshot.gates})
            if snapshot.gates is not None
            else None
        )
        rules = (
            sorted({f"{key}@{version}" for key, version in snapshot.rules})
            if snapshot.rules is not None
            else None
        )
        if not rows or gates != previous_gates or rules != previous_rules:
            rows.append(
                {
                    "day": snapshot.day.isoformat(),
                    "gates": gates,
                    "rules": rules,
                    "added": sorted(set(rules or ()) - set(previous_rules or ())),
                    "removed": sorted(set(previous_rules or ()) - set(rules or ())),
                }
            )
        previous_gates, previous_rules = gates, rules
    return rows


def _component_table(snapshot: Snapshot) -> list[dict[str, Any]]:
    return [
        {
            "component": item.name,
            "score": item.score,
            "weight": item.weight,
            "product": item.score * item.weight,
        }
        for item in snapshot.components or ()
    ]


def attribution(snapshots: Mapping[date, Snapshot], bands: Sequence[Any]) -> dict[str, Any]:
    ordered = [snapshot for _, snapshot in sorted(snapshots.items())]
    rows = attribution_rows(snapshots, bands)
    gate_days: Counter[str] = Counter()
    changes, confidence = [], []
    previous_label = None
    previous_confidence = None
    capped_days, model_days = 0, 0
    for i, (snapshot, row) in enumerate(zip(ordered, rows, strict=True)):
        model, final = snapshot.bands["model"], snapshot.bands[FINAL]
        if model is not None and final is not None:
            model_days += 1
            capped_days += final.high < model.high - EPSILON
        ids = row["binding"]
        extra = [key for key in ids or () if key != SCORE_MODEL]
        gate_days.update(extra if extra else [row["label"]])
        if row["label"] != previous_label:
            changes.append({"day": row["day"], "label": row["label"], "reasons": row["reasons"]})
        previous_label = row["label"]
        active = None if ids is None else "confidence" in ids
        if i in (0, len(ordered) - 1) or active != previous_confidence:
            confidence.append(
                {
                    "day": row["day"],
                    "binding": active,
                    "score": snapshot.confidence_score,
                    "level": snapshot.confidence_level,
                    "reasons": list(snapshot.confidence_reasons)
                    if snapshot.confidence_reasons is not None
                    else None,
                }
            )
        previous_confidence = active
    with_components = [snapshot for snapshot in ordered if snapshot.components]
    components: dict[str, Any] = {
        "first": None,
        "last": None,
        "first_table": [],
        "last_table": [],
        "moves": [],
    }
    if with_components:
        first, last = with_components[0], with_components[-1]
        first_table, last_table = _component_table(first), _component_table(last)
        initial = {row["component"]: row["product"] for row in first_table}
        moves = [
            {"component": row["component"], "change": row["product"] - initial[row["component"]]}
            for row in last_table
            if row["component"] in initial
        ]
        components.update(
            first=first.day.isoformat(),
            last=last.day.isoformat(),
            first_table=first_table,
            last_table=last_table,
            moves=sorted(moves, key=lambda row: -abs(row["change"]))[:3],
        )
    return {
        "rows": rows,
        "capped_days": capped_days,
        "model_days": model_days,
        "gate_days": dict(sorted(gate_days.items())),
        "components": components,
        "changes": changes,
        "confidence": confidence,
        "versions": versions(ordered),
    }


def _previous_trading_day(day: date) -> date:
    day -= timedelta(days=1)
    while not is_us_equity_trading_day(day):
        day -= timedelta(days=1)
    return day


def f0001_first_needed(start: date) -> date:
    previous = _previous_trading_day(start)
    month_end = _previous_trading_day(previous)
    while True:
        following = month_end + timedelta(days=1)
        while not is_us_equity_trading_day(following):
            following += timedelta(days=1)
        if following.month != month_end.month:
            break
        month_end = _previous_trading_day(month_end)
    # F0001 fast_slow 的最长回看是 252 个交易日；恰好满足同一个执行函数的预热。
    for _ in range(252):
        month_end = _previous_trading_day(month_end)
    return month_end


def build(
    daily_roots: Sequence[Path],
    *,
    research_root: Path = fl.RESEARCH_ROOT,
    rules_path: Path = DEFAULT_SCORING_RULES_CONFIG_PATH,
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not daily_roots:
        raise ScorecardError("no daily root")
    rules = load_scoring_rules(rules_path)
    step = float(rules.position_change.minimum_action_delta)
    selection = select(daily_roots)
    if not selection.snapshots:
        raise ScorecardError("no usable decision snapshot")
    prices = load_prices(daily_roots[0])
    start, end = min(selection.snapshots), pd.Timestamp(prices.qqq.index[-1]).date()
    days = fl.trading_days(start, end)
    if len(days) < 3:
        raise ScorecardError(f"no held day between {start} and the last price day {end}")
    f0001_error = None
    try:
        overlay = fl.overlay_exposure(prices.qqq, start, end, research_root)
    except fl.ForwardLedgerError as exc:
        overlay = None
        f0001_error = str(exc)
    held = held_days(days, selection.snapshots)
    stale = stale_days(held)
    lines = exposures(days, held, selection.snapshots, overlay)
    returns = [fl.day_return(prices.qqq, day) for day in days]
    cash = [fl.day_return(prices.cash, day) for day in days] if not prices.cash.empty else None
    full = list(range(2, len(days)))
    fresh = [i for i in full if not stale[i]]
    passes = {}
    for name, indices in (("full", full), ("fresh", fresh)):
        passes[name] = {"zero": pass_stats(lines, returns, indices, step=step)}
        if cash is not None:
            passes[name]["sgov"] = pass_stats(lines, returns, indices, step=step, cash=cash)
    event_rows = events(days, lines[FINAL], held, returns, step=step)
    down = event_summary(event_rows, "down", returns, 2)
    up = event_summary(event_rows, "up", returns, 2)
    snapshots = {day: snapshot for day, snapshot in selection.snapshots.items() if day <= end}
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "code": dict(provenance or {}),
        "roots": selection.roots,
        "window": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "first_held": days[2].isoformat(),
            "days": len(full),
            "stale_days": sum(stale[i] for i in full),
        },
        "gaps": gaps(days, snapshots),
        "unusable": selection.unusable,
        "overlaps": selection.overlaps,
        "missing": {
            field: sum(field in snapshot.missing for snapshot in snapshots.values())
            for field in FIELDS
        },
        "identity": identity(snapshots),
        "prices": {
            "file": prices.file,
            "sha256": prices.sha256,
            "first": pd.Timestamp(prices.qqq.index[0]).date().isoformat(),
            "last": end.isoformat(),
            "cash": cash is not None,
            "f0001_first_needed": f0001_first_needed(start).isoformat(),
        },
        "f0001_error": f0001_error,
        "config": {
            "file": str(rules_path),
            "boundaries": boundaries(rules.position_bands),
            "step": step,
        },
        "passes": passes,
        "monthly": monthly(days, lines, returns, full),
        "events": {"rows": event_rows, "down": down, "up": up},
        "attribution": attribution(snapshots, rules.position_bands),
        "reading": reading(passes, down),
    }


def _fmt(value: Any, spec: str = ".3f") -> str:
    return "–" if value is None else format(value, spec)


def _points(value: Any, *, signed: bool = False, missing: str = "–") -> str:
    return missing if value is None else format(value * 100, "+.2f" if signed else ".2f")


def _interval(value: Any) -> str:
    if value is None:
        return "–"
    if isinstance(value, str):
        return value
    return f"{value[0]:.2f}–{value[1]:.2f}"


def _yes(value: Any) -> str:
    return "是" if value else "否"


def _table(lines: list[str], headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    lines += ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]


def _reasons(reasons: Mapping[str, Any]) -> str:
    return (
        "；".join(
            f"`{key}`：{value if value is not None else MISSING}" for key, value in reasons.items()
        )
        or NO_MATCH
    )


def _render_inputs(result: Mapping[str, Any], lines: list[str]) -> None:
    lines += ["", "## 输入", ""]
    _table(
        lines,
        ("目录", "快照文件", "用了", "非交易日的快照"),
        [
            [
                f"{i + 1}. `{root['path']}`",
                root["files"],
                root["used"],
                "、".join(root["other_days"]) or "没有",
            ]
            for i, root in enumerate(result["roots"])
        ],
    )
    window, prices, config = result["window"], result["prices"], result["config"]
    lines += [
        "",
        f"- 窗口 {window['start']}..{window['end']}，第一个有持有敞口的天 {window['first_held']}，"
        f"持有 {window['days']} 天，陈旧 {window['stale_days']} 天"
        f"（持有快照比当天早 {STALE_LAG} 个交易日及以上；正常是 2）。",
        f"- 价格文件 `{prices['file']}`，sha256 `{prices['sha256']}`，"
        f"QQQ {prices['first']}..{prices['last']}；"
        f"F0001 需要的第一个日期 {prices['f0001_first_needed']}。",
    ]
    if result["f0001_error"] is not None:
        lines.append(f"- F0001 没算出来：{result['f0001_error']}。第 5 条线和它的对照都是空的。")
    edges = "、".join(f"{edge:g}" for edge in config["boundaries"])
    lines += [
        f"- 分档边界 {edges}，换仓步长 {config['step']:g}（`minimum_action_delta`），"
        f"取自 `{config['file']}`。",
        "",
        f"### 断档（连续 {GAP_DAYS} 个交易日及以上没有可用快照）",
        "",
    ]
    lines += [f"- {row['first']}..{row['last']}，{row['days']} 天" for row in result["gaps"]] or [
        "- 没有。"
    ]
    lines += ["", "### 用不了的快照", ""]
    lines += [
        f"- {row['day']}（目录 {row['root'] + 1}）：{row['reason']}" for row in result["unusable"]
    ] or ["- 没有。"]
    overlaps = result["overlaps"]
    different = [row for row in overlaps if row["differ"]]
    lines += [
        "",
        "### 两个目录都有的天",
        "",
        f"共 {len(overlaps)} 天，不同 {len(different)} 天；用目录 1 的。",
    ]
    for row in different:
        left, right = row["roots"]
        lines.append(
            f"- {row['day']}：目录 {left + 1} {_interval(row['bands'][0])}，"
            f"目录 {right + 1} {_interval(row['bands'][1])}。"
        )
    lines += ["", "### 字段缺失（窗口内的可用快照）", ""]
    _table(lines, ("字段", "缺的天数"), [[field, result["missing"][field]] for field in FIELDS])
    check = result["identity"]
    mismatch = "（" + "、".join(check["mismatch"]) + "）" if check["mismatch"] else ""
    lines += [
        "",
        "### 恒等核对",
        "",
        f"核了 {check['checked']} 份，没法核 {check['unchecked']} 份，"
        f"不符 {len(check['mismatch'])} 份{mismatch}。",
        "",
        "### 各条线的假设",
        "",
    ]
    lines += [f"{i}. {LABELS[line]}：{ASSUMPTIONS[line]}" for i, line in enumerate(LINES, 1)]
    lines.append("- 同平均敞口的固定：敞口取那条线在同一组交集天上的平均敞口。")


def _render_passes(result: Mapping[str, Any], lines: list[str]) -> None:
    for name, title in (("full", "全窗口（陈旧天照沿用算）"), ("fresh", "去掉陈旧天")):
        passes = result["passes"][name]
        lines += ["", f"## {title}，现金按 0", ""]
        rows = []
        for line in LINES:
            stats = passes["zero"][line]
            rows.append(
                [
                    LABELS[line],
                    stats["days"],
                    _points(stats["log"]),
                    _points(stats["versus_fixed"], signed=True),
                    _points(stats.get("versus_own"), signed=True),
                    _points(stats.get("noise")),
                    _fmt(stats.get("units"), ".2f"),
                    _fmt(stats["mean_exposure"]),
                    _points(stats["max_drawdown"]),
                    stats["changes"],
                    _fmt(stats["turnover"]),
                ]
            )
        _table(
            lines,
            (
                "线",
                "天数",
                "累计",
                "对固定 0.8",
                "对同平均敞口固定",
                "噪声尺度",
                "噪声单位",
                "平均敞口",
                "最大回撤",
                "换仓次数",
                "累计换手",
            ),
            rows,
        )
        lines += [
            "",
            "点数是对数点（×100）。噪声尺度 sd(e − ē) × sd(r) × √N，描述，不是检验；"
            "噪声单位 = 对同平均敞口固定的差 ÷ 噪声尺度。",
        ]
        if "sgov" not in passes:
            lines += ["", "价格文件里没有 SGOV，现金按 SGOV 一组不印。"]
            continue
        lines += ["", f"### {title}，现金按 SGOV（只印）", ""]
        _table(
            lines,
            ("线", "天数", "累计", "对固定 0.8", "对同平均敞口固定"),
            [
                [
                    LABELS[line],
                    passes["sgov"][line]["days"],
                    _points(passes["sgov"][line]["log"]),
                    _points(passes["sgov"][line]["versus_fixed"], signed=True),
                    _points(passes["sgov"][line].get("versus_own"), signed=True),
                ]
                for line in LINES
            ],
        )


def _render_reading(result: Mapping[str, Any], lines: list[str]) -> None:
    reading_result = result["reading"]
    lines += ["", "## 跑之前写死的读法（需求第 5.1 节，这里只机械算条件）", ""]
    for i, line in enumerate((FINAL, "total_asset"), 1):
        full = result["passes"]["full"]["zero"][line]["versus_own"]
        fresh = result["passes"]["fresh"]["zero"][line]["versus_own"]
        lines.append(
            f"- 第 {i} 条：全窗口 {_points(full, signed=True)}、"
            f"去掉陈旧天 {_points(fresh, signed=True)}；"
            f"两遍都 ≤ −1.00：{_yes(reading_result['a'][line])}。"
        )
    lines.append(f"- 甲成立：{_yes(reading_result['a']['holds'])}。")
    down = result["events"]["down"]
    above = reading_result["b"]["above_base"]
    comparison = "对不了" if above is None else ("高于" if above else "不高于")
    lines.append(
        f"- 乙：计数的 {down['counted']} 次，为正 {down['positive']}/{down['counted']}，"
        f"至少 6 次且 ≥ 2/3：{_yes(reading_result['b']['holds'])}；"
        f"比例{comparison}底数 {_fmt(down['base'], '.0%')}。"
    )


def _render_events(result: Mapping[str, Any], lines: list[str]) -> None:
    lines += ["", "## 按月（现金按 0，对数点）", ""]
    _table(
        lines,
        ("月", *(LABELS[line] for line in LINES), "第 1 条平均敞口", "第 2 条平均敞口"),
        [
            [
                row["month"],
                *(_points(row[line]) for line in LINES),
                _fmt(row["final_exposure"]),
                _fmt(row["total_asset_exposure"]),
            ]
            for row in result["monthly"]
        ],
    )
    lines += [
        "",
        "## 降仓和加仓（第 1 条，中点变动 ≥ 换仓步长）",
        "",
        "之后 5 天、20 天从新敞口生效那天起；断档后照印不计；未满不进比例。",
        "",
    ]
    _table(
        lines,
        ("日期", "方向", "从", "到", "前后持有快照", "断档后", "之后 5 天", "之后 20 天"),
        [
            [
                row["day"],
                "降" if row["kind"] == "down" else "加",
                _fmt(row["from"]),
                _fmt(row["to"]),
                " → ".join(day or MISSING for day in row["snapshots"]),
                "是" if row["after_gap"] else "",
                _points(row["next_5"], missing="未满"),
                _points(row["next_20"], missing="未满"),
            ]
            for row in result["events"]["rows"]
        ],
    )
    lines.append("")
    rows = []
    for kind, label in (("down", "降仓"), ("up", "加仓")):
        summary = result["events"][kind]
        rows.append(
            [
                label,
                summary["events"],
                summary["after_gap"],
                summary["not_full"],
                summary["counted"],
                f"{summary['positive']}（{_fmt(summary['share'], '.0%')}）",
                _points(summary["median"]["20"]),
                _points(summary["median"]["5"]),
                summary["non_overlapping"],
                f"{_fmt(summary['base'], '.0%')}（{summary['base_days']} 天）",
            ]
        )
    _table(
        lines,
        (
            "方向",
            "次数",
            "断档后",
            "未满",
            "计数的",
            "20 天为正",
            "20 天中位数",
            "5 天中位数",
            "互不重叠",
            "底数",
        ),
        rows,
    )
    lines += [
        "",
        "底数是从第一个持有日起，每一天之后满 20 天的 QQQ 收益为正的比例；收益是简单收益（×100）。",
    ]


def _render_attribution(result: Mapping[str, Any], lines: list[str]) -> None:
    summary = result["attribution"]
    capped, count = summary["capped_days"], summary["model_days"]
    lines += [
        "",
        "## 拆因（照快照原样印，不重算评分）",
        "",
        f"最终上限低于模型上限：{capped}/{count}"
        f"（{_fmt(capped / count if count else None, '.0%')}）。",
        "起作用的闸门是 triggered 为真且上限与最终上限相同的闸门；"
        "只剩 score_model 记「无额外闸门」；置信度只压上限。",
        "",
    ]
    _table(lines, ("起作用的闸门", "天数"), list(summary["gate_days"].items()))
    lines += ["", "### 每天", ""]
    rows = []
    for row in summary["rows"]:
        caps = "、".join(
            f"{key}（{_fmt(value)}）" for key, value in row["caps"].items() if value is not None
        )
        label = row["label"] + (f"：{caps}" if caps else "")
        rows.append(
            [
                row["day"],
                row["root"] + 1,
                _interval(row["model"]),
                label,
                _interval(row["final"]),
                _interval(row["budget"]),
                row["macro_level"] or MISSING,
                _fmt(row["score"], ".1f"),
                _fmt(row["boundary"], "g"),
                _fmt(row["distance"], "+.1f"),
                row["mapping"],
            ]
        )
    _table(
        lines,
        (
            "日期",
            "目录",
            "模型区间",
            "起作用的闸门（上限）",
            "最终区间",
            "总风险资产预算",
            "宏观预算",
            "综合分",
            "最近的分档边界",
            "距离",
            "映射",
        ),
        rows,
    )
    components = summary["components"]
    for name in ("first", "last"):
        lines += ["", f"### 分项（{components[name] or MISSING}）", ""]
        _table(
            lines,
            ("分项", "分数", "权重", "分数 × 权重"),
            [
                [
                    row["component"],
                    _fmt(row["score"], ".1f"),
                    _fmt(row["weight"], ".1f"),
                    _fmt(row["product"], ".1f"),
                ]
                for row in components[f"{name}_table"]
            ],
        )
    lines += ["", "分数 × 权重变动最大的三个：", ""]
    lines += [f"- {row['component']}：{row['change']:+.1f}" for row in components["moves"]] or [
        "- 没有可比的分项。"
    ]
    lines += ["", "### 起作用闸门的原文（第一份、换闸门那天）", ""]
    lines += [
        f"- {row['day']}（{row['label']}）：{_reasons(row['reasons'])}"
        for row in summary["changes"]
    ]
    if summary["rows"]:
        last = summary["rows"][-1]
        lines.append(f"- 最新一份 {last['day']}（{last['label']}）：{_reasons(last['reasons'])}")
    lines += ["", "### 置信度（第一份、最新一份、`confidence` 闸门起不起作用变了的日子）", ""]
    for row in summary["confidence"]:
        active = MISSING if row["binding"] is None else ("起作用" if row["binding"] else "不起作用")
        reasons = "；".join(row["reasons"]) if row["reasons"] is not None else MISSING
        lines.append(
            f"- {row['day']}：{active}，{_fmt(row['score'], '.1f')}"
            f"（{row['level'] or MISSING}）：{reasons}"
        )
    lines += ["", "### 版本表（闸门集合或规则版本变了的日子）", ""]
    for row in summary["versions"]:
        gates = MISSING if row["gates"] is None else "、".join(row["gates"])
        rules = MISSING if row["rules"] is None else f"{len(row['rules'])} 条"
        added, removed = "、".join(row["added"]) or "无", "、".join(row["removed"]) or "无"
        lines.append(f"- {row['day']}：闸门 {gates}；规则 {rules}；加 {added}；减 {removed}")


def render(result: Mapping[str, Any]) -> str:
    code = result["code"]
    lines = [
        "# 日报仓位成绩单",
        "",
        f"- 运行：{result['run_id']}；依据 `{result['owner_decision']}`（第 6 期章程第 21 节）。",
        f"- 代码：{code.get('commit')}（改动：{code.get('code_modified')}）。",
        f"- {NOT_A_READ}窗口只有约 5 个月，不能说谁更会判断。",
    ]
    _render_inputs(result, lines)
    _render_passes(result, lines)
    _render_reading(result, lines)
    _render_events(result, lines)
    _render_attribution(result, lines)
    return "\n".join([*lines, "", "production_effect=none；broker_action=none。", ""])


def write_result(result: Mapping[str, Any], out_dir: Path) -> tuple[Path, Path]:
    summary, report = out_dir / SUMMARY, out_dir / REPORT
    text = json.dumps(result, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n"
    rendered = render(result)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary.write_text(text, encoding="utf-8", newline="\n")
    report.write_text(rendered, encoding="utf-8", newline="\n")
    return summary, report


@app.callback()
def callback() -> None:
    """日报仓位成绩单（TRADING-2565-80）。"""


@app.command("run")
def run_command(
    daily_root: Annotated[list[Path], typer.Option(help="日报状态根目录，只读，可以重复。")],
    out: Annotated[Path, typer.Option(help="成绩单输出目录。")],
) -> None:
    try:
        result = build(daily_root, provenance=git_provenance(PROJECT_ROOT))
    except ScorecardError as exc:
        typer.echo(f"daily_band_scorecard: 读不了：{exc}")
        raise typer.Exit(code=1) from None
    window = result["window"]
    used = "，".join(
        f"目录 {i + 1} 用了 {root['used']} 天" for i, root in enumerate(result["roots"])
    )
    typer.echo(
        f"daily_band_scorecard: {window['start']}..{window['end']}，"
        f"第一个持有日 {window['first_held']}；{used}"
    )
    if result["f0001_error"] is not None:
        typer.echo(f"daily_band_scorecard: F0001 没算出来：{result['f0001_error']}")
    summary, report = write_result(result, out)
    typer.echo(f"daily_band_scorecard: 已写 {summary}")
    typer.echo(f"daily_band_scorecard: 已写 {report}")


if __name__ == "__main__":
    app()
