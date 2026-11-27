"""Acceptance tests for TRADING-2565-142: the daily-band scorecard reads the gated keys.

Requirement: knowledge qqq-options/auto-loop/period8/gate-line-requirements-2026-11-27.md
section 5 (period 8 charter 3.4 / 5.1, owner_decision:TRADING-2565:2026-11-27:research_loop_p8).
A snapshot with ``gated_risk_asset_ai_band`` reads the old-rule bands; other snapshots read as
before, and a set without any gated key prints exactly what the code printed before this run.
Synthetic files only.
"""

from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.config import DEFAULT_SCORING_RULES_CONFIG_PATH
from ai_trading_system.research_probes import daily_band_scorecard as sc
from ai_trading_system.research_probes import forward_ledger as fl

FIXTURES = Path(__file__).parent / "fixtures" / "trading_2565_142"
GOLDEN_JSON = FIXTURES / "daily_band_scorecard.json"
GOLDEN_MD = FIXTURES / "daily_band_scorecard.md"
PROVENANCE = {"commit": "golden", "code_modified": False}
DECISION = "owner_decision:TRADING-2565:2026-11-27:research_loop_p8"
ROOT_MARK = "<DAILY_ROOT>"
RULES_MARK = "<SCORING_RULES>"

PRICE_START = date(2025, 1, 2)
FIRST = date(2026, 6, 1)
LAST_SNAPSHOT = date(2026, 8, 28)
LAST_PRICE = date(2026, 9, 3)
CAPTURE = date(2026, 9, 4)
BAD_DAY = date(2026, 6, 17)
NO_TOTAL_DAY = date(2026, 6, 24)
GAP = (date(2026, 7, 13), date(2026, 7, 16))
SWITCH = date(2026, 8, 3)
# (model low, model high, final high, budget low, budget high, score), cycled by trading day.
PATTERN = (
    (0.4, 0.6, 0.6, 0.6, 0.8, 55.0),
    (0.4, 0.6, 0.4, 0.6, 0.8, 55.0),
    (0.4, 0.6, 0.4, 0.6, 0.8, 55.0),
    (0.2, 0.4, 0.4, 0.5, 0.7, 40.0),
    (0.2, 0.4, 0.3, 0.5, 0.7, 40.0),
    (0.6, 0.8, 0.7, 0.6, 0.8, 70.0),
    (0.6, 0.8, 0.8, 0.6, 0.8, 70.0),
    (0.6, 0.8, 0.4, 0.35, 0.55, 70.0),
    (0.0, 0.2, 0.2, 0.35, 0.55, 20.0),
)
FULL = {"min_position": 1.0, "max_position": 1.0, "label": "满仓（默认）"}


def _price_days() -> list[date]:
    return fl.trading_days(PRICE_START, LAST_PRICE)


def _prices(base: float, scale: int) -> pd.Series:
    # Integer rules only, so every platform writes the same ten decimals.
    days = _price_days()
    returns = [base + (((k * 37) % 29) - 14) / scale for k in range(len(days))]
    returns[0] = 0.0
    return pd.Series(100.0 * np.cumprod([1.0 + r for r in returns]), index=pd.DatetimeIndex(days))


QQQ = _prices(0.0006, 1000)
SGOV = _prices(0.00015, 200000)


def _pattern(day: date) -> tuple[float, float, float, float, float, float]:
    return PATTERN[len(fl.trading_days(FIRST, day)) % len(PATTERN)]


def _band(low: float, high: float, label: str | None = None) -> dict[str, Any]:
    band: dict[str, Any] = {"min_position": low, "max_position": high}
    if label is not None:
        band["label"] = label
    return band


def record(day: date, *, policy: str | None = None) -> dict[str, Any]:
    """A snapshot as the daily writes it: old format, gated_min with keys, or full exposure."""
    model_low, model_high, final_high, budget_low, budget_high, score = _pattern(day)
    final = (min(model_low, final_high), final_high)
    total = (final[0] * budget_low, final[1] * budget_high)
    gates = [
        {"gate_id": "score_model", "max_position": model_high, "triggered": True, "reason": "m"},
        {
            "gate_id": "valuation",
            "max_position": final_high,
            "triggered": final_high < model_high - 1e-9,
            "reason": "估值分位",
        },
    ]
    positions: dict[str, Any] = {
        "model_risk_asset_ai_band": _band(model_low, model_high, "模型"),
        "final_risk_asset_ai_band": _band(*final, "中性"),
        "final_total_risk_asset_band": _band(budget_low, budget_high, "总风险资产预算"),
        "total_asset_ai_band": _band(*total, "中性"),
        "macro_risk_asset_budget": {"level": "normal" if budget_low == 0.6 else "stress"},
        "position_gates": gates,
    }
    if policy is not None:
        positions["gated_risk_asset_ai_band"] = _band(*final, "中性")
        positions["gated_total_asset_ai_band"] = _band(*total, "中性")
        positions["position_policy"] = {"mode": policy, "owner_decision": "x"}
    if policy == "full_exposure_default":
        positions["final_risk_asset_ai_band"] = dict(FULL)
        positions["total_asset_ai_band"] = dict(FULL)
        positions["final_total_risk_asset_band"] = _band(1.0, 1.0, "总风险资产（满仓）")
        for gate in gates:
            gate["effect"] = "info_only"
    return {
        "scores": {
            "overall_score": score,
            "confidence_score": 70.0,
            "confidence_level": "medium",
            "confidence_reasons": ["数据新鲜"],
            "components": [
                {"component": "trend", "score": score, "weight": 25},
                {"component": "valuation", "score": 50.0, "weight": 10},
            ],
        },
        "positions": positions,
        "rule_versions": {"rules": [{"rule_id": "r1", "version": 1}]},
    }


def _write(root: Path, day: date, payload: dict[str, Any] | str) -> None:
    path = root / sc.SNAPSHOT_DIR / f"decision_snapshot_{day.isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    path.write_bytes(text.encode("utf-8"))


def _write_capture(root: Path) -> None:
    path = root / fl.PRICES_REL.format(day=CAPTURE.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = ["date,ticker,open,high,low,close,adj_close,volume"]
    for stamp in QQQ.index:
        text = stamp.date().isoformat()
        rows.append(f"{text},QQQ,1,1,1,1,{QQQ[stamp]:.10f},100")
        rows.append(f"{text},SGOV,1,1,1,1,{SGOV[stamp]:.10f},100")
    path.write_bytes(("\n".join(rows) + "\n").encode("utf-8"))


def build_root(root: Path, *, after_switch: str | None = None) -> Path:
    """The golden set; after_switch rewrites the snapshots from SWITCH in that policy."""
    for day in fl.trading_days(FIRST, LAST_SNAPSHOT):
        if GAP[0] <= day <= GAP[1]:
            continue
        if day == BAD_DAY:
            _write(root, day, "{not json")
            continue
        payload = record(day, policy=after_switch if day >= SWITCH else None)
        if day == NO_TOTAL_DAY:
            del payload["positions"]["total_asset_ai_band"]
        _write(root, day, payload)
    _write_capture(root)
    return root


def normalized(result: dict[str, Any], root: Path) -> tuple[dict[str, Any], str]:
    """JSON and Markdown with the machine-specific paths replaced by placeholders."""
    markdown = (
        sc.render(result)
        .replace(str(root), ROOT_MARK)
        .replace(str(DEFAULT_SCORING_RULES_CONFIG_PATH), RULES_MARK)
    )
    data = json.loads(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    for item in data["roots"]:
        item["path"] = ROOT_MARK
    data["config"]["file"] = RULES_MARK
    return data, markdown


def _close(left: Any, right: Any, where: str = "$") -> None:
    if isinstance(left, float) or isinstance(right, float):
        assert isinstance(left, int | float) and isinstance(right, int | float), where
        assert not isinstance(left, bool) and not isinstance(right, bool), where
        assert math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-15), (where, left, right)
    elif isinstance(left, dict):
        assert isinstance(right, dict) and left.keys() == right.keys(), where
        for key in left:
            _close(left[key], right[key], f"{where}.{key}")
    elif isinstance(left, list):
        assert isinstance(right, list) and len(left) == len(right), where
        for i, (a, b) in enumerate(zip(left, right, strict=True)):
            _close(a, b, f"{where}[{i}]")
    else:
        assert left == right, (where, left, right)


def _build(root: Path) -> dict[str, Any]:
    return sc.build([root], provenance=PROVENANCE)


@pytest.fixture(scope="module")
def old_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_root(tmp_path_factory.mktemp("old"))


@pytest.fixture(scope="module")
def full_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_root(tmp_path_factory.mktemp("full"), after_switch="full_exposure_default")


@pytest.fixture(scope="module")
def gated_min_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_root(tmp_path_factory.mktemp("gated_min"), after_switch="gated_min")


def _parse(payload: dict[str, Any]) -> sc.Snapshot:
    return sc.parse_snapshot(json.dumps(payload, ensure_ascii=False), SWITCH, 0, "f.json")


def _gated(final: tuple[float, float], total: tuple[float, float] | None) -> dict[str, Any]:
    payload = record(SWITCH, policy="full_exposure_default")
    payload["positions"]["gated_risk_asset_ai_band"] = _band(*final, "防守")
    if total is None:
        del payload["positions"]["gated_total_asset_ai_band"]
    else:
        payload["positions"]["gated_total_asset_ai_band"] = _band(*total, "防守")
    return payload


# Constants ---------------------------------------------------------------------------------------


def test_the_gated_constants_sit_beside_the_old_ones() -> None:
    assert sc.GATED_FIELDS == {
        "final": "gated_risk_asset_ai_band",
        "total_asset": "gated_total_asset_ai_band",
    }
    assert sc.GATED_DECISION == DECISION
    # The old names, labels and run id stay: old outputs must not change by a byte.
    assert sc.RUN_ID == "TRADING-2565-80"
    assert sc.BAND_FIELDS["final"] == "final_risk_asset_ai_band"
    assert sc.BAND_FIELDS["total_risk"] == "final_total_risk_asset_band"
    assert sc.LABELS["final"] == "日报·风险资产内 AI 区间中点"


# Old snapshots print exactly what they printed before ---------------------------------------------


def test_snapshots_without_gated_keys_print_the_same_json_and_markdown_as_before(
    old_root: Path,
) -> None:
    result = _build(old_root)
    assert "gated" not in result
    data, markdown = normalized(result, old_root)
    assert markdown == GOLDEN_MD.read_text(encoding="utf-8")
    _close(data, json.loads(GOLDEN_JSON.read_text(encoding="utf-8")))
    assert "### 换包以后读 gated 键" not in markdown


def test_a_snapshot_without_the_gated_key_parses_as_before() -> None:
    snapshot = _parse(record(SWITCH))
    assert snapshot.source == "final"
    model_low, model_high, final_high, budget_low, budget_high, _ = _pattern(SWITCH)
    final = (min(model_low, final_high), final_high)
    assert snapshot.bands["final"] == sc.Bounds(*final)
    assert snapshot.bands["total_risk"] == sc.Bounds(budget_low, budget_high)
    assert snapshot.bands["total_asset"] == sc.Bounds(final[0] * budget_low, final[1] * budget_high)


# Reading a snapshot with the gated keys ----------------------------------------------------------


def test_a_full_exposure_snapshot_reads_the_old_rule_bands_and_divides_end_by_end() -> None:
    snapshot = _parse(_gated((0.2, 0.4), (0.07, 0.22)))
    assert snapshot.source == "gated"
    assert snapshot.bands["final"] == sc.Bounds(0.2, 0.4)
    assert snapshot.bands["total_asset"] == sc.Bounds(0.07, 0.22)
    budget = snapshot.bands["total_risk"]
    assert budget is not None
    # The 2026-11-20 pair: 0.07 / 0.2 and 0.22 / 0.4, the stress budget 0.35-0.55.
    assert budget.low == pytest.approx(0.35, abs=1e-12)
    assert budget.high == pytest.approx(0.55, abs=1e-12)
    assert budget.mid != pytest.approx((0.07 + 0.22) / (0.2 + 0.4))  # not midpoint over midpoint
    model_low, model_high, *_ = _pattern(SWITCH)
    assert snapshot.bands["model"] == sc.Bounds(model_low, model_high)
    assert "total_asset_ai_band" not in snapshot.missing
    assert "final_total_risk_asset_band" not in snapshot.missing


@pytest.mark.parametrize(
    ("final", "total"),
    [((0.0, 0.0), (0.0, 0.0)), ((0.0, 0.2), (0.0, 0.11))],
)
def test_a_gate_band_with_a_zero_end_leaves_the_budget_line_missing(
    final: tuple[float, float], total: tuple[float, float]
) -> None:
    snapshot = _parse(_gated(final, total))
    assert snapshot.bands["final"] == sc.Bounds(*final)  # exposure 0 is a valid reading
    assert snapshot.bands["total_asset"] == sc.Bounds(*total)
    assert snapshot.bands["total_risk"] is None
    assert "final_total_risk_asset_band" in snapshot.missing


def test_a_quotient_above_one_leaves_the_budget_line_missing() -> None:
    # 1.5 > 1.25 (bounds out of order) and 0.5 <= 1.25 (in order, top above one).
    for total in ((0.3, 0.5), (0.1, 0.5)):
        snapshot = _parse(_gated((0.2, 0.4), total))
        assert snapshot.bands["total_asset"] == sc.Bounds(*total)
        assert snapshot.bands["total_risk"] is None, total


def test_a_missing_or_invalid_gated_total_blanks_lines_two_and_six() -> None:
    for total in (None, (0.3, 0.1)):
        snapshot = _parse(_gated((0.2, 0.4), total))
        assert snapshot.bands["final"] == sc.Bounds(0.2, 0.4)
        assert snapshot.bands["total_asset"] is None
        assert snapshot.bands["total_risk"] is None
        assert {"total_asset_ai_band", "final_total_risk_asset_band"} <= set(snapshot.missing)


@pytest.mark.parametrize(
    "value",
    [
        None,
        "0.2-0.4",
        {"min_position": 0.6, "max_position": 0.4},
        {"min_position": 0.2},
        {"min_position": 0.2, "max_position": 1.2},
        {"min_position": True, "max_position": 0.4},
    ],
)
def test_an_invalid_gated_band_makes_the_snapshot_unusable_without_falling_back(
    value: Any,
) -> None:
    payload = record(SWITCH, policy="full_exposure_default")
    payload["positions"]["gated_risk_asset_ai_band"] = value
    with pytest.raises(ValueError) as caught:
        _parse(payload)
    assert str(caught.value) == (
        "gated_risk_asset_ai_band: missing or invalid min_position or max_position"
    )


def test_the_gated_path_does_not_need_the_final_band() -> None:
    payload = _gated((0.2, 0.4), (0.07, 0.22))
    del payload["positions"]["final_risk_asset_ai_band"]
    assert _parse(payload).bands["final"] == sc.Bounds(0.2, 0.4)


# Whole runs --------------------------------------------------------------------------------------


def _strip(result: dict[str, Any]) -> dict[str, Any]:
    """Everything that must read the same whichever way the old-rule bands were stored."""
    data: dict[str, Any] = json.loads(
        json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False)
    )
    for key in ("roots", "identity", "missing", "gated"):
        data.pop(key, None)
    for name in ("full", "fresh"):
        for cash in data["passes"][name].values():
            cash.pop("total_risk")
    for row in data["monthly"]:
        row.pop("total_risk")
    for row in data["attribution"]["rows"]:
        row.pop("budget")
    return data


def _zero_low_days(root: Path) -> list[str]:
    days = []
    for day in fl.trading_days(SWITCH, LAST_SNAPSHOT):
        model_low, _, final_high, *_ = _pattern(day)
        if min(model_low, final_high) == 0.0:
            days.append(day.isoformat())
    return days


@pytest.mark.parametrize("policy", ["full_exposure_default", "gated_min"])
def test_switched_snapshots_read_as_the_old_rule_would_have(
    policy: str, old_root: Path, full_root: Path, gated_min_root: Path
) -> None:
    root = full_root if policy == "full_exposure_default" else gated_min_root
    old, new = _build(old_root), _build(root)
    # Lines 1-5, events, the monthly table, binding and caps: the same, to float noise.
    _close(_strip(new), _strip(old))
    # Line 6 is divided back; on gated days with a zero low end it is missing.
    rows = {row["day"]: row for row in new["attribution"]["rows"]}
    old_rows = {row["day"]: row for row in old["attribution"]["rows"]}
    zero = set(_zero_low_days(root))
    assert zero
    for day, row in rows.items():
        if day in zero:
            assert row["budget"] is None, day
        else:
            assert row["budget"] == pytest.approx(old_rows[day]["budget"], abs=1e-12), day
    assert new["missing"]["final_total_risk_asset_band"] == len(zero)


def test_binding_on_a_full_exposure_snapshot_finds_the_old_rule_gate(full_root: Path) -> None:
    rows = {row["day"]: row for row in _build(full_root)["attribution"]["rows"]}
    for day in fl.trading_days(SWITCH, LAST_SNAPSHOT):
        model_low, model_high, final_high, *_ = _pattern(day)
        row = rows[day.isoformat()]
        assert row["final"] == [min(model_low, final_high), final_high]
        expected = "valuation" if final_high < model_high - 1e-9 else sc.NO_EXTRA
        assert row["label"] == expected, day


def test_the_identity_check_counts_only_old_snapshots(old_root: Path, full_root: Path) -> None:
    old, new = _build(old_root)["identity"], _build(full_root)["identity"]
    gated = len([d for d in fl.trading_days(SWITCH, LAST_SNAPSHOT)])
    assert new["checked"] + new["unchecked"] == old["checked"] + old["unchecked"] - gated
    assert new["mismatch"] == []


def test_the_gated_block_in_the_json(full_root: Path) -> None:
    result = _build(full_root)
    days = fl.trading_days(SWITCH, LAST_SNAPSHOT)
    assert result["gated"] == {
        "owner_decision": DECISION,
        "snapshots": len(days),
        "first": SWITCH.isoformat(),
        "last": LAST_SNAPSHOT.isoformat(),
        "total_risk_missing": _zero_low_days(full_root),
    }


def test_the_gated_subsection_in_the_markdown(full_root: Path) -> None:
    markdown = sc.render(_build(full_root))
    days = fl.trading_days(SWITCH, LAST_SNAPSHOT)
    missing = "、".join(_zero_low_days(full_root))
    expected = (
        "### 换包以后读 gated 键\n"
        "\n"
        f"{len(days)} 份快照有 `gated_risk_asset_ai_band`（{SWITCH}..{LAST_SNAPSHOT}），"
        f"按 `{DECISION}` 第 3.4 节读按原规则算出的区间：第 1 条线读 `gated_risk_asset_ai_band`，"
        "第 2 条读 `gated_total_asset_ai_band`，第 6 条按两者逐端相除推，"
        f"闸门区间有一端是 0 的那天记缺（{missing}）。这些快照不进恒等核对。"
        "别的快照照旧读最终区间。\n"
    )
    assert markdown.count("### 换包以后读 gated 键") == 1
    assert expected in markdown
    assert markdown.index("### 恒等核对") < markdown.index(expected)
    assert markdown.index(expected) < markdown.index("### 各条线的假设")


def test_the_subsection_says_none_when_no_budget_is_missing(tmp_path: Path) -> None:
    root = tmp_path / "daily"
    days = [d for d in fl.trading_days(SWITCH, LAST_SNAPSHOT) if _pattern(d)[0] > 0]
    for day in fl.trading_days(FIRST, SWITCH)[:-1]:
        _write(root, day, record(day))
    for day in days:
        _write(root, day, record(day, policy="full_exposure_default"))
    _write_capture(root)
    result = _build(root)
    assert result["gated"]["total_risk_missing"] == []
    assert "闸门区间有一端是 0 的那天记缺（没有）。" in sc.render(result)


def test_an_unusable_gated_snapshot_is_listed_with_its_reason(tmp_path: Path) -> None:
    root = build_root(tmp_path / "daily", after_switch="full_exposure_default")
    broken = fl.trading_days(SWITCH, LAST_SNAPSHOT)[3]
    payload = record(broken, policy="full_exposure_default")
    payload["positions"]["gated_risk_asset_ai_band"] = {"min_position": 0.5, "max_position": 0.2}
    _write(root, broken, payload)
    result = _build(root)
    assert {
        "day": broken.isoformat(),
        "root": 0,
        "reason": "gated_risk_asset_ai_band: missing or invalid min_position or max_position",
    } in result["unusable"]
    assert result["gated"]["snapshots"] == len(fl.trading_days(SWITCH, LAST_SNAPSHOT)) - 1
