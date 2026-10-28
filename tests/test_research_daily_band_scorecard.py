"""TRADING-2565-80: the daily-band scorecard on synthetic daily roots."""

from __future__ import annotations

import ast
import json
import math
import statistics
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import DEFAULT_SCORING_RULES_CONFIG_PATH, load_scoring_rules
from ai_trading_system.research_probes import daily_band_scorecard as sc
from ai_trading_system.research_probes import forward_ledger as fl

SOURCE = Path(sc.__file__)
BANDS = load_scoring_rules(DEFAULT_SCORING_RULES_CONFIG_PATH).position_bands
PRICE_START = date(2025, 1, 2)
FIRST = date(2026, 5, 1)
ARCHIVE_LAST = date(2026, 9, 4)
PROD_FIRST = date(2026, 9, 1)
CAPTURE = date(2026, 10, 23)
LAST_PRICE = date(2026, 10, 22)
BAD_DAY = date(2026, 6, 10)
NO_MODEL_DAY = date(2026, 6, 15)
DIFFER_DAY = date(2026, 9, 2)
GAP = (date(2026, 8, 3), date(2026, 8, 14))
# (model low, model high, final high, budget low, budget high), cycled by trading-day position.
PATTERN = (
    (0.4, 0.6, 0.6, 0.6, 0.8),
    (0.4, 0.6, 0.5, 0.6, 0.8),
    (0.4, 0.6, 0.5, 0.6, 0.8),
    (0.2, 0.4, 0.4, 0.5, 0.7),
    (0.2, 0.4, 0.3, 0.5, 0.7),
    (0.6, 0.8, 0.7, 0.6, 0.8),
    (0.6, 0.8, 0.8, 0.6, 0.8),
)
SCORES = {0.2: 40.0, 0.4: 55.0, 0.6: 70.0}


def _returns(days: list[date], base: float, amplitude: float) -> np.ndarray[Any, Any]:
    return np.array([base + amplitude * math.sin(k / 5.0 + 1.0) for k in range(len(days))])


PRICE_DAYS = fl.trading_days(PRICE_START, LAST_PRICE)
QQQ = pd.Series(
    100 * np.cumprod(1 + _returns(PRICE_DAYS, 0.0007, 0.012)), index=pd.DatetimeIndex(PRICE_DAYS)
)
SGOV = pd.Series(
    100 * np.cumprod(1 + _returns(PRICE_DAYS, 0.00015, 0.0001)), index=pd.DatetimeIndex(PRICE_DAYS)
)


def _write_capture(
    daily_root: Path, folder: date, last: date, first: date = PRICE_START, cash: bool = True
) -> None:
    path = daily_root / fl.PRICES_REL.format(day=folder.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = ["date,ticker,open,high,low,close,adj_close,volume"]
    for stamp in QQQ.index:
        if pd.Timestamp(first) <= stamp <= pd.Timestamp(last):
            text = stamp.date().isoformat()
            rows.append(f"{text},QQQ,1,1,1,1,{QQQ[stamp]:.10f},100")
            if cash:
                rows.append(f"{text},SGOV,1,1,1,1,{SGOV[stamp]:.10f},100")
    path.write_bytes(("\n".join(rows) + "\n").encode("utf-8"))


def _pattern(day: date) -> tuple[float, float, float, float, float]:
    return PATTERN[fl.trading_days(FIRST, day).__len__() % len(PATTERN)]


def snapshot_record(
    model: tuple[float, float],
    final: tuple[float, float],
    budget: tuple[float, float],
    *,
    score: float = 55.0,
    gates: list[Any] | None = None,
    rules: list[dict[str, Any]] | None = None,
    confidence: float = 72.5,
) -> dict[str, Any]:
    if gates is None:
        gates = [
            {"gate_id": "score_model", "max_position": model[1], "triggered": True, "reason": "m"},
            {
                "gate_id": "confidence",
                "max_position": final[1],
                "triggered": final[1] < model[1] - 1e-9,
                "reason": f"判断置信度 {confidence}/100",
            },
        ]
    return {
        "schema_version": 1,
        "scores": {
            "overall_score": score,
            "confidence_score": confidence,
            "confidence_level": "medium",
            "confidence_reasons": ["数据新鲜"],
            "components": [
                {"component": "trend", "score": score, "weight": 25},
                {"component": "valuation", "score": 50.0, "weight": 10},
            ],
        },
        "positions": {
            "model_risk_asset_ai_band": {"min_position": model[0], "max_position": model[1]},
            "final_risk_asset_ai_band": {"min_position": final[0], "max_position": final[1]},
            "final_total_risk_asset_band": {"min_position": budget[0], "max_position": budget[1]},
            "total_asset_ai_band": {
                "min_position": final[0] * budget[0],
                "max_position": final[1] * budget[1],
            },
            "macro_risk_asset_budget": {"level": "normal" if budget[0] == 0.6 else "elevated"},
            "position_gates": gates,
        },
        "rule_versions": {
            "rules": rules if rules is not None else [{"rule_id": "r1", "version": 1}]
        },
    }


def _record(day: date) -> dict[str, Any]:
    model_low, model_high, final_high, budget_low, budget_high = _pattern(day)
    record = snapshot_record(
        (model_low, model_high),
        (min(model_low, final_high), final_high),
        (budget_low, budget_high),
        score=SCORES[model_low],
        rules=[{"rule_id": "r1", "version": 1 if day < date(2026, 7, 1) else 2}],
    )
    if day == NO_MODEL_DAY:
        del record["positions"]["model_risk_asset_ai_band"]
    return record


def write_snapshot(daily_root: Path, day: date, record: dict[str, Any] | str) -> None:
    path = daily_root / sc.SNAPSHOT_DIR / f"decision_snapshot_{day.isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    text = record if isinstance(record, str) else json.dumps(record, ensure_ascii=False)
    path.write_bytes(text.encode("utf-8"))


@pytest.fixture(scope="module")
def roots(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    base = tmp_path_factory.mktemp("daily")
    prod, archive = base / "prod", base / "archive"
    for day in fl.trading_days(FIRST, ARCHIVE_LAST):
        if GAP[0] <= day <= GAP[1]:
            continue
        write_snapshot(archive, day, "{not json" if day == BAD_DAY else _record(day))
    write_snapshot(archive, date(2026, 7, 4), _record(date(2026, 7, 2)))
    for day in fl.trading_days(PROD_FIRST, CAPTURE):
        record = _record(day)
        if day == DIFFER_DAY:
            record["positions"]["final_risk_asset_ai_band"]["max_position"] = 0.55
            record["positions"]["total_asset_ai_band"]["max_position"] = 0.55 * 0.8
        write_snapshot(prod, day, record)
    _write_capture(prod, CAPTURE, LAST_PRICE)
    _write_capture(prod, date(2026, 10, 20), date(2026, 10, 19))
    _write_capture(archive, ARCHIVE_LAST, ARCHIVE_LAST)
    return prod, archive


@pytest.fixture(scope="module")
def result(roots: tuple[Path, Path]) -> dict[str, Any]:
    return sc.build(list(roots), provenance={"commit": "abc", "code_modified": False})


def _v(value: float | None) -> float:
    assert value is not None
    return value


def _mid(record: dict[str, Any], field: str) -> float:
    band = record["positions"][field]
    return float(band["min_position"] + band["max_position"]) / 2.0


# Constants and source ----------------------------------------------------------------------------


def test_constants() -> None:
    assert sc.RUN_ID == "TRADING-2565-80"
    assert sc.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-28:daily_band_scorecard"
    assert sc.SNAPSHOT_DIR == "data/processed/decision_snapshots"
    assert sc.CAPTURE_DIR == "data/raw/daily_input_capture"
    assert sc.PRICES_NAME == "market_macro/prices_daily.csv"
    assert sc.LINES == ("final", "total_asset", "model", "fixed", "f0001", "total_risk")
    assert sc.OWN_FIXED == ("final", "total_asset", "model", "f0001")
    assert sc.BAND_FIELDS == {
        "final": "final_risk_asset_ai_band",
        "total_asset": "total_asset_ai_band",
        "model": "model_risk_asset_ai_band",
        "total_risk": "final_total_risk_asset_band",
    }
    assert (sc.STALE_LAG, sc.GAP_DAYS, sc.HORIZONS) == (5, 3, (5, 20))
    assert (sc.LINE_A, sc.EVENTS_B, sc.SHARE_B, sc.EPSILON) == (-0.01, 6, (2, 3), 1e-9)
    assert (sc.SUMMARY, sc.REPORT) == ("daily_band_scorecard.json", "daily_band_scorecard.md")
    assert sc.FIELDS == (
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


def test_the_module_only_reads_and_reuses_the_forward_ledger() -> None:
    text = SOURCE.read_text(encoding="utf-8")
    tree = ast.parse(text)
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert "from ai_trading_system.research_probes import forward_ledger as fl" in text
    assert imported <= {
        "__future__",
        "collections",
        "collections.abc",
        "dataclasses",
        "datetime",
        "pathlib",
        "typing",
        "ai_trading_system.config",
        "ai_trading_system.core.provenance",
        "ai_trading_system.research_probes",
        "ai_trading_system.trading_calendar",
    }
    for name in ("fl.overlay_exposure(", "fl.read_capture(", "fl.day_return(", "fl.trading_days("):
        assert name in text
    for banned in (
        "read_band",
        "build_rows",
        "ledger.jsonl",
        "def simulate(",
        "def overlay_exposure(",
        "def trading_days(",
        "def read_capture(",
        "def day_return(",
        ".unlink(",
        "write_decision_snapshot",
    ):
        assert banned not in text
    assert text.count(".write_text(") == 2


# Reading one snapshot ----------------------------------------------------------------------------


def test_a_full_snapshot_reads_every_field() -> None:
    record = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8), score=55.0)
    snapshot = sc.parse_snapshot(json.dumps(record), date(2026, 6, 1), 1, "f.json")
    assert (snapshot.day, snapshot.root, snapshot.file) == (date(2026, 6, 1), 1, "f.json")
    assert snapshot.bands["final"] == sc.Bounds(0.4, 0.5)
    assert snapshot.bands["model"] == sc.Bounds(0.4, 0.6)
    assert snapshot.bands["total_risk"] == sc.Bounds(0.6, 0.8)
    assert snapshot.bands["total_asset"] == sc.Bounds(0.4 * 0.6, 0.5 * 0.8)
    assert snapshot.bands["final"].mid == pytest.approx(0.45)
    assert snapshot.macro_level == "normal"
    assert snapshot.gates == (
        sc.Gate("score_model", 0.6, True, "m"),
        sc.Gate("confidence", 0.5, True, "判断置信度 72.5/100"),
    )
    assert (snapshot.overall_score, snapshot.confidence_score) == (55.0, 72.5)
    assert snapshot.confidence_level == "medium"
    assert snapshot.confidence_reasons == ("数据新鲜",)
    assert snapshot.components == (
        sc.Component("trend", 55.0, 25.0),
        sc.Component("valuation", 50.0, 10.0),
    )
    assert snapshot.rules == (("r1", "1"),)
    assert snapshot.missing == ()


def test_a_snapshot_without_the_model_band_reads_with_the_field_missing() -> None:
    record = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8))
    del record["positions"]["model_risk_asset_ai_band"]
    del record["scores"]["confidence_reasons"]
    record["positions"]["total_asset_ai_band"] = {"min_position": 0.7, "max_position": 0.2}
    record["rule_versions"] = None
    snapshot = sc.parse_snapshot(json.dumps(record), date(2026, 6, 1), 0, "f.json")
    assert snapshot.bands["model"] is None
    assert snapshot.bands["total_asset"] is None
    assert snapshot.bands["final"] == sc.Bounds(0.4, 0.5)
    assert snapshot.missing == (
        "model_risk_asset_ai_band",
        "total_asset_ai_band",
        "confidence_reasons",
        "rule_versions.rules",
    )


def test_an_early_snapshot_with_only_the_final_band_reads() -> None:
    record = {"positions": {"final_risk_asset_ai_band": {"min_position": 0, "max_position": 0.2}}}
    snapshot = sc.parse_snapshot(json.dumps(record), date(2026, 5, 1), 0, "f.json")
    assert snapshot.bands["final"] == sc.Bounds(0.0, 0.2)
    assert snapshot.gates is None and snapshot.components is None and snapshot.rules is None
    assert snapshot.missing == sc.FIELDS


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("{not json", "invalid JSON"),
        ("[1, 2]", "snapshot is not a dict"),
        ('{"positions": 3}', "positions is not a dict"),
        (
            '{"positions": {}}',
            "final_risk_asset_ai_band: missing or invalid min_position or max_position",
        ),
        (
            json.dumps(
                {"positions": {sc.BAND_FIELDS["final"]: {"min_position": 0.6, "max_position": 0.4}}}
            ),
            "final_risk_asset_ai_band: missing or invalid min_position or max_position",
        ),
        (
            json.dumps(
                {"positions": {sc.BAND_FIELDS["final"]: {"min_position": True, "max_position": 1}}}
            ),
            "final_risk_asset_ai_band: missing or invalid min_position or max_position",
        ),
    ],
)
def test_an_unusable_snapshot_says_why(text: str, message: str) -> None:
    with pytest.raises(ValueError) as caught:
        sc.parse_snapshot(text, date(2026, 6, 1), 0, "f.json")
    assert str(caught.value) == message


def test_gate_and_component_entries_that_do_not_parse_are_dropped_or_blank() -> None:
    record = snapshot_record(
        (0.4, 0.6),
        (0.4, 0.5),
        (0.6, 0.8),
        gates=[
            {"gate_id": "valuation", "max_position": "0.5", "triggered": 1, "reason": None},
            {"max_position": 0.5},
            "x",
        ],
    )
    record["scores"]["components"].append({"component": "macro", "score": None, "weight": 15})
    snapshot = sc.parse_snapshot(json.dumps(record), date(2026, 6, 1), 0, "f.json")
    assert snapshot.gates == (sc.Gate("valuation", None, None, None),)
    assert [item.name for item in snapshot.components or ()] == ["trend", "valuation"]


# Roots -------------------------------------------------------------------------------------------


def test_snapshot_files_lists_trading_days_and_sets_other_days_aside(tmp_path: Path) -> None:
    write_snapshot(tmp_path, date(2026, 6, 1), "{}")
    write_snapshot(tmp_path, date(2026, 6, 6), "{}")
    (tmp_path / sc.SNAPSHOT_DIR / "decision_snapshot_2026-06-01.json.bak").write_text("x")
    (tmp_path / sc.SNAPSHOT_DIR / "notes.txt").write_text("x")
    files, other = sc.snapshot_files(tmp_path)
    assert files == {date(2026, 6, 1): f"{sc.SNAPSHOT_DIR}/decision_snapshot_2026-06-01.json"}
    assert other == ["decision_snapshot_2026-06-06.json"]


def test_a_root_that_does_not_exist_stops_the_table(tmp_path: Path) -> None:
    with pytest.raises(sc.ScorecardError, match="no such directory"):
        sc.snapshot_files(tmp_path / "nope")
    assert sc.snapshot_files(tmp_path) == ({}, [])


def test_each_day_comes_from_the_first_root_that_has_the_file(tmp_path: Path) -> None:
    first, second = tmp_path / "a", tmp_path / "b"
    same = snapshot_record((0.4, 0.6), (0.4, 0.6), (0.6, 0.8))
    other = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8))
    write_snapshot(first, date(2026, 6, 1), same)
    write_snapshot(second, date(2026, 6, 1), same)
    write_snapshot(first, date(2026, 6, 2), same)
    write_snapshot(second, date(2026, 6, 2), other)
    write_snapshot(second, date(2026, 6, 3), other)
    write_snapshot(first, date(2026, 6, 4), "{bad")
    write_snapshot(second, date(2026, 6, 4), same)
    write_snapshot(first, date(2026, 6, 5), same)
    write_snapshot(second, date(2026, 6, 5), snapshot_record((0.4, 0.6), (0.3, 0.6), (0.6, 0.8)))
    selection = sc.select([first, second])
    assert {day: s.root for day, s in selection.snapshots.items()} == {
        date(2026, 6, 1): 0,
        date(2026, 6, 2): 0,
        date(2026, 6, 3): 1,
        date(2026, 6, 5): 0,
    }
    assert selection.snapshots[date(2026, 6, 2)].bands["final"] == sc.Bounds(0.4, 0.6)
    assert selection.unusable == [{"day": "2026-06-04", "root": 0, "reason": "invalid JSON"}]
    assert selection.overlaps == [
        {"day": "2026-06-01", "roots": [0, 1], "bands": [[0.4, 0.6], [0.4, 0.6]], "differ": False},
        {"day": "2026-06-02", "roots": [0, 1], "bands": [[0.4, 0.6], [0.4, 0.5]], "differ": True},
        {
            "day": "2026-06-04",
            "roots": [0, 1],
            "bands": ["invalid JSON", [0.4, 0.6]],
            "differ": False,
        },
        {"day": "2026-06-05", "roots": [0, 1], "bands": [[0.4, 0.6], [0.3, 0.6]], "differ": True},
    ]
    assert [(root["files"], root["used"]) for root in selection.roots] == [(4, 3), (5, 1)]
    assert selection.roots[0]["path"] == str(first)


def test_the_price_file_is_the_latest_capture_of_the_first_root(roots: tuple[Path, Path]) -> None:
    prod, archive = roots
    assert sc.find_prices(prod) == fl.PRICES_REL.format(day=CAPTURE.isoformat())
    prices = sc.load_prices(prod)
    assert prices.file == fl.PRICES_REL.format(day=CAPTURE.isoformat())
    assert pd.Timestamp(prices.qqq.index[-1]).date() == LAST_PRICE
    assert prices.qqq.iloc[-1] == pytest.approx(QQQ.iloc[-1], rel=1e-9)
    assert prices.cash.iloc[-1] == pytest.approx(SGOV.iloc[-1], rel=1e-9)
    import hashlib

    raw = (prod / prices.file).read_bytes()
    assert prices.sha256 == hashlib.sha256(raw).hexdigest()
    with pytest.raises(sc.ScorecardError, match="no data/raw/daily_input_capture"):
        sc.find_prices(archive.parent)


# Days --------------------------------------------------------------------------------------------

DAYS = fl.trading_days(date(2026, 6, 1), date(2026, 6, 12))


def test_held_days_follow_t_plus_two_and_carry_forward() -> None:
    snapshots = {DAYS[i]: object() for i in (0, 1, 7)}
    held = sc.held_days(DAYS, snapshots)  # type: ignore[arg-type]
    assert held == [None, None, 0, 1, 1, 1, 1, 1, 1, 7]
    assert sc.stale_days(held) == [False] * 6 + [True, True, True, False]
    assert sc.gaps(DAYS, snapshots) == [  # type: ignore[arg-type]
        {"first": "2026-06-03", "last": "2026-06-09", "days": 5}
    ]


def test_a_run_of_two_missing_days_is_not_a_gap() -> None:
    snapshots = {DAYS[i]: object() for i in (0, 1, 2, 5, 6, 7, 8)}
    assert sc.gaps(DAYS, snapshots) == []  # type: ignore[arg-type]
    snapshots = {DAYS[i]: object() for i in (0, 1, 2, 6, 7, 8, 9)}
    assert sc.gaps(DAYS, snapshots) == [  # type: ignore[arg-type]
        {"first": "2026-06-04", "last": "2026-06-08", "days": 3}
    ]
    snapshots = {DAYS[i]: object() for i in (0, 1, 2)}
    assert sc.gaps(DAYS, snapshots) == [  # type: ignore[arg-type]
        {"first": "2026-06-04", "last": "2026-06-12", "days": 7}
    ]


def _snapshot(day: date, record: dict[str, Any]) -> sc.Snapshot:
    return sc.parse_snapshot(json.dumps(record), day, 0, "f.json")


def test_exposures_take_every_band_from_the_held_snapshot_and_never_look_further_back() -> None:
    full = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.5, 0.7))
    bare = snapshot_record((0.2, 0.4), (0.2, 0.3), (0.6, 0.8))
    del bare["positions"]["model_risk_asset_ai_band"]
    del bare["positions"]["total_asset_ai_band"]
    snapshots = {DAYS[0]: _snapshot(DAYS[0], full), DAYS[2]: _snapshot(DAYS[2], bare)}
    held = sc.held_days(DAYS[:6], snapshots)
    overlay = pd.Series([0.3, 0.6, 0.9], index=pd.DatetimeIndex(DAYS[2:5]))
    lines = sc.exposures(DAYS[:6], held, snapshots, overlay)
    assert lines["final"] == [None, None, 0.45, 0.45, 0.25, 0.25]
    assert lines["model"] == [None, None, 0.5, 0.5, None, None]
    assert lines["total_asset"] == pytest.approx([None, None, 0.275, 0.275, None, None])
    assert lines["total_risk"] == [None, None, 0.6, 0.6, 0.7, 0.7]
    assert lines["fixed"] == [None, None, 0.8, 0.8, 0.8, 0.8]
    assert lines["f0001"] == [None, None, 0.3, 0.6, 0.9, None]
    assert sc.exposures(DAYS[:3], held[:3], snapshots, None)["f0001"] == [None, None, None]


# One line ----------------------------------------------------------------------------------------

E = [None, None, 0.5, 0.5, 0.3, None, 0.4, 0.45]
R = [0.01, 0.02, 0.01, -0.02, 0.03, 0.01, None, -0.01]
C = [0.0001, 0.0001, 0.0002, None, 0.0002, 0.0002, 0.0002, 0.0002]


def test_line_stats_on_the_days_with_exposure_and_return() -> None:
    stats = sc.line_stats(E, R, [2, 3, 4, 5, 6, 7], step=0.05)
    present = [2, 3, 4, 7]
    own = sum(math.log1p(_v(E[i]) * _v(R[i])) for i in present)
    fixed = sum(math.log1p(0.8 * _v(R[i])) for i in present)
    mean = (0.5 + 0.5 + 0.3 + 0.45) / 4
    same = sum(math.log1p(mean * _v(R[i])) for i in present)
    assert stats["days"] == 4
    assert stats["log"] == pytest.approx(own, abs=1e-15)
    assert stats["versus_fixed"] == pytest.approx(own - fixed, abs=1e-15)
    assert stats["mean_exposure"] == pytest.approx(mean, abs=1e-15)
    assert stats["versus_own"] == pytest.approx(own - same, abs=1e-15)
    scale = (
        statistics.stdev([0.5, 0.5, 0.3, 0.45])
        * statistics.stdev([0.01, -0.02, 0.03, -0.01])
        * math.sqrt(4)
    )
    assert stats["noise"] == pytest.approx(scale, abs=1e-15)
    assert stats["units"] == pytest.approx((own - same) / scale, abs=1e-12)
    wealth = np.cumprod([1 + _v(E[i]) * _v(R[i]) for i in present])
    peak = np.maximum.accumulate(np.concatenate([[1.0], wealth]))[1:]
    assert stats["max_drawdown"] == pytest.approx(min(0.0, float(np.min(wealth / peak - 1))))
    # Adjacent days only: 2→3 (0), 3→4 (0.2) and 6→7 (0.05, counted though 0.45 − 0.4 is below
    # 0.05 in floating point); 4→6 is not adjacent.
    assert stats["changes"] == 2
    assert stats["turnover"] == pytest.approx(0.25)


def test_changes_skip_a_day_left_out_of_the_pass() -> None:
    exposure = [None, None, 0.5, 0.5, 0.3, 0.6, 0.6]
    stats = sc.line_stats(exposure, [0.01] * 7, [2, 3, 5, 6], step=0.05)
    assert (stats["days"], stats["changes"], stats["turnover"]) == (4, 0, 0)


def test_the_drawdown_starts_from_the_window_start() -> None:
    stats = sc.line_stats([0.5, 0.5, 0.5], [-0.02, 0.01, -0.03], [0, 1, 2], step=0.05)
    assert stats["max_drawdown"] == pytest.approx(0.99 * 1.005 * 0.985 - 1)


def test_the_same_average_fixed_line_holds_the_line_s_own_average() -> None:
    stats = sc.line_stats(E, R, [2, 3, 4, 5, 6, 7], step=0.05)
    mean = stats["mean_exposure"]
    present = [2, 3, 4, 7]
    flat = [mean if i in present else None for i in range(len(E))]
    again = sc.line_stats(flat, R, [2, 3, 4, 5, 6, 7], step=0.05)
    assert again["mean_exposure"] == pytest.approx(mean)
    assert again["versus_own"] == pytest.approx(0.0, abs=1e-15)
    assert again["noise"] == 0.0 and again["units"] is None
    assert again["log"] - again["versus_own"] == pytest.approx(stats["log"] - stats["versus_own"])


def test_line_stats_with_cash_needs_the_cash_return_too() -> None:
    stats = sc.line_stats(E, R, [2, 3, 4, 5, 6, 7], step=0.05, cash=C)
    present = [2, 4, 7]

    def day(exposure: float, i: int) -> float:
        return exposure * _v(R[i]) + (1 - exposure) * _v(C[i])

    own = sum(math.log1p(day(_v(E[i]), i)) for i in present)
    mean = (0.5 + 0.3 + 0.45) / 3
    assert stats["days"] == 3
    assert stats["log"] == pytest.approx(own, abs=1e-15)
    assert stats["versus_fixed"] == pytest.approx(
        own - sum(math.log1p(day(0.8, i)) for i in present), abs=1e-15
    )
    assert stats["versus_own"] == pytest.approx(
        own - sum(math.log1p(day(mean, i)) for i in present), abs=1e-15
    )


def test_a_line_without_its_own_fixed_and_an_empty_line() -> None:
    fixed = sc.line_stats([None, 0.8, 0.8], [0.01, 0.02, -0.01], [1, 2], step=0.05, own_fixed=False)
    assert "versus_own" not in fixed and "noise" not in fixed
    assert fixed["versus_fixed"] == 0.0 and fixed["changes"] == 0
    empty = sc.line_stats([None, None], [0.01, 0.02], [0, 1], step=0.05)
    assert empty == {
        "days": 0,
        "log": None,
        "versus_fixed": None,
        "mean_exposure": None,
        "max_drawdown": None,
        "changes": 0,
        "turnover": 0,
        "versus_own": None,
        "noise": None,
        "units": None,
    }


def test_noise_scale() -> None:
    assert sc.noise_scale([0.5], [0.01]) is None
    assert sc.noise_scale([0.2, 0.4, 0.9], [0.01, -0.03, 0.02]) == pytest.approx(
        statistics.stdev([0.2, 0.4, 0.9]) * statistics.stdev([0.01, -0.03, 0.02]) * math.sqrt(3)
    )


# Identity, events --------------------------------------------------------------------------------


def test_the_identity_check_finds_a_band_that_is_not_final_times_budget() -> None:
    good = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8))
    bad = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8))
    bad["positions"]["total_asset_ai_band"]["max_position"] = 0.5 * 0.8 + 2e-9
    low = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8))
    low["positions"]["total_asset_ai_band"]["min_position"] = 0.4 * 0.8
    unchecked = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8))
    del unchecked["positions"]["final_total_risk_asset_band"]
    close = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8))
    close["positions"]["total_asset_ai_band"]["max_position"] = 0.5 * 0.8 + 5e-10
    records = [good, bad, low, unchecked, close]
    snapshots = {DAYS[i]: _snapshot(DAYS[i], record) for i, record in enumerate(records)}
    assert sc.identity(snapshots) == {
        "checked": 4,
        "unchecked": 1,
        "mismatch": ["2026-06-02", "2026-06-03"],
    }


def test_forward_return_starts_on_the_effect_day() -> None:
    returns = [0.01, None, 0.02, 0.03, -0.01, 0.04]
    assert sc.forward_return(returns, 2, 3) == pytest.approx(1.02 * 1.03 * 0.99 - 1)
    assert sc.forward_return(returns, 3, 3) == pytest.approx(1.03 * 0.99 * 1.04 - 1)
    assert sc.forward_return(returns, 4, 3) is None
    assert sc.forward_return(returns, 0, 2) is None


EVENT_DAYS = fl.trading_days(date(2026, 6, 1), date(2026, 7, 31))
EVENT_RETURNS: list[float | None] = [
    0.004 * math.sin(k / 2.0) + 0.0005 * k / 10 for k in range(len(EVENT_DAYS))
]


def test_events_are_moves_of_at_least_the_step_with_the_effect_day_return() -> None:
    n = len(EVENT_DAYS)
    mid: list[float | None] = [None, None] + [0.5] * (n - 2)
    for index in range(5, n):
        mid[index] = 0.45
    for index in range(10, n):
        mid[index] = 0.6
    for index in range(12, n):
        mid[index] = 0.3
    for index in range(13, n):
        mid[index] = 0.27
    for index in range(30, n):
        mid[index] = 0.2
    held: list[int | None] = [None, None, *range(n - 2)]
    held[12] = 10
    held[11] = 4
    rows = sc.events(EVENT_DAYS, mid, held, EVENT_RETURNS, step=0.05)
    assert [(row["index"], row["kind"], row["after_gap"]) for row in rows] == [
        (5, "down", False),
        (10, "up", False),
        (12, "down", True),
        (30, "down", False),
    ]
    first = rows[0]
    assert first["day"] == EVENT_DAYS[5].isoformat()
    assert (first["from"], first["to"]) == (0.5, 0.45)
    assert first["snapshots"] == [EVENT_DAYS[2].isoformat(), EVENT_DAYS[3].isoformat()]
    assert rows[2]["snapshots"] == [EVENT_DAYS[4].isoformat(), EVENT_DAYS[10].isoformat()]
    expected = math.prod(1 + _v(r) for r in EVENT_RETURNS[5:25]) - 1
    assert first["next_20"] == pytest.approx(expected, abs=1e-15)
    assert first["next_5"] == pytest.approx(
        math.prod(1 + _v(r) for r in EVENT_RETURNS[5:10]) - 1,
        abs=1e-15,
    )
    assert rows[3]["next_20"] is None and rows[3]["next_5"] is not None


def test_a_gap_of_exactly_three_days_makes_the_next_move_after_gap() -> None:
    days = EVENT_DAYS[:8]
    mid: list[float | None] = [None, None, 0.5, 0.5, 0.5, 0.5, 0.5, 0.4]
    held: list[int | None] = [None, None, 0, 1, 2, 2, 2, 6]
    assert sc.events(days, mid, held, EVENT_RETURNS[:8], step=0.05)[0]["after_gap"] is True
    held = [None, None, 0, 1, 2, 2, 2, 5]
    assert sc.events(days, mid, held, EVENT_RETURNS[:8], step=0.05)[0]["after_gap"] is False


def _row(index: int, kind: str = "down", gap: bool = False, next_20: float | None = 0.01) -> Any:
    return {
        "index": index,
        "kind": kind,
        "after_gap": gap,
        "next_5": None if next_20 is None else next_20 / 4,
        "next_20": next_20,
    }


def test_event_summary_counts_full_events_off_the_gap_and_the_base_rate() -> None:
    rows = [
        _row(3, next_20=0.02),
        _row(10, next_20=-0.01),
        _row(12, gap=True, next_20=0.5),
        _row(23, next_20=0.03),
        _row(30, next_20=0.01),
        _row(41, "up", next_20=0.07),
        _row(43, next_20=-0.02),
        _row(50, next_20=None),
    ]
    returns: list[float | None] = [0.01 if k % 3 else -0.02 for k in range(60)]
    summary = sc.event_summary(rows, "down", returns, 2)
    assert summary["events"] == 7
    assert summary["after_gap"] == 1
    assert summary["not_full"] == 1
    assert summary["counted"] == 5
    assert summary["positive"] == 3
    assert summary["share"] == pytest.approx(0.6)
    assert summary["median"]["20"] == pytest.approx(0.01)
    assert summary["median"]["5"] == pytest.approx(0.0025)
    assert summary["non_overlapping"] == 3
    full = [sc.forward_return(returns, i, 20) for i in range(2, 60)]
    values = [value for value in full if value is not None]
    assert summary["base_days"] == len(values) == 39
    assert summary["base"] == pytest.approx(sum(value > 0 for value in values) / 39)
    up = sc.event_summary(rows, "up", returns, 2)
    assert (up["events"], up["counted"], up["positive"], up["non_overlapping"]) == (1, 1, 1, 1)
    none = sc.event_summary([], "down", returns, 2)
    assert none["share"] is None and none["median"] == {"5": None, "20": None}


# Reading conditions ------------------------------------------------------------------------------


def _passes(full: float | None, fresh: float | None, other: float = 0.0) -> Any:
    def one(value: float | None) -> Any:
        return {"zero": {"final": {"versus_own": value}, "total_asset": {"versus_own": other}}}

    return {"full": one(full), "fresh": one(fresh)}


def _down(counted: int, positive: int, share_base: float | None = 0.6) -> Any:
    return {
        "counted": counted,
        "positive": positive,
        "share": positive / counted if counted else None,
        "base": share_base,
    }


def test_the_reading_needs_both_passes_for_a_and_six_events_for_b() -> None:
    assert sc.reading(_passes(-0.012, -0.010), _down(0, 0))["a"] == {
        "final": True,
        "total_asset": False,
        "holds": True,
    }
    assert sc.reading(_passes(-0.012, -0.009), _down(0, 0))["a"]["holds"] is False
    assert sc.reading(_passes(None, -0.02), _down(0, 0))["a"]["final"] is False
    assert sc.reading(_passes(0.0, 0.0, other=-0.03), _down(0, 0))["a"]["holds"] is True
    assert sc.reading(_passes(0, 0), _down(6, 4))["b"] == {"holds": True, "above_base": True}
    assert sc.reading(_passes(0, 0), _down(9, 6, 0.7))["b"] == {
        "holds": True,
        "above_base": False,
    }
    assert sc.reading(_passes(0, 0), _down(6, 3))["b"]["holds"] is False
    assert sc.reading(_passes(0, 0), _down(5, 5))["b"]["holds"] is False
    assert sc.reading(_passes(0, 0), _down(0, 0))["b"] == {"holds": False, "above_base": None}


# Attribution -------------------------------------------------------------------------------------


def _gate(gate_id: str, cap: float, triggered: bool = True) -> dict[str, Any]:
    return {"gate_id": gate_id, "max_position": cap, "triggered": triggered, "reason": gate_id}


def test_binding_gates_are_all_triggered_gates_at_the_final_cap() -> None:
    record = snapshot_record(
        (0.4, 0.6),
        (0.4, 0.5),
        (0.6, 0.8),
        gates=[
            _gate("score_model", 0.6),
            _gate("confidence", 0.5),
            _gate("valuation", 0.5 + 5e-10),
            _gate("data_confidence", 0.5, triggered=False),
            {"gate_id": "risk_events", "max_position": 0.5, "triggered": "yes", "reason": "x"},
            _gate("thesis", 0.7),
        ],
    )
    snapshot = _snapshot(DAYS[0], record)
    assert sc.binding(snapshot) == ["confidence", "valuation"]
    assert sc.binding_label(["confidence", "valuation"]) == "confidence、valuation"
    assert sc.binding_label(["score_model"]) == "无额外闸门"
    assert sc.binding_label(["score_model", "risk_budget"]) == "risk_budget"
    assert sc.binding_label([]) == "没有对上的闸门"
    assert sc.binding_label(None) == "缺"
    record["positions"]["position_gates"] = "x"
    assert sc.binding(_snapshot(DAYS[0], record)) is None


def test_attribution_rows_print_the_boundary_distance_and_a_mapping_that_does_not_fit() -> None:
    plain = snapshot_record((0.4, 0.6), (0.4, 0.6), (0.6, 0.8), score=48.3)
    tie = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.5, 0.7), score=57.5)
    tie["positions"]["macro_risk_asset_budget"]["level"] = "elevated"
    bare = snapshot_record((0.4, 0.6), (0.4, 0.6), (0.6, 0.8), score=50.0)
    del bare["positions"]["model_risk_asset_ai_band"]
    snapshots = {
        DAYS[0]: _snapshot(DAYS[0], plain),
        DAYS[1]: _snapshot(DAYS[1], tie),
        DAYS[2]: _snapshot(DAYS[2], bare),
    }
    rows = sc.attribution_rows(snapshots, BANDS)
    assert [row["day"] for row in rows] == ["2026-06-01", "2026-06-02", "2026-06-03"]
    assert (rows[0]["boundary"], rows[0]["mapping"]) == (50.0, "映射不符")
    assert rows[0]["distance"] == pytest.approx(-1.7)
    assert rows[0]["label"] == "无额外闸门"
    assert rows[0]["caps"] == {"score_model": 0.6}
    assert (rows[1]["boundary"], rows[1]["distance"], rows[1]["mapping"]) == (50.0, 7.5, "符合")
    assert rows[1]["label"] == "confidence"
    assert rows[1]["reasons"] == {"confidence": "判断置信度 72.5/100"}
    assert rows[1]["model"] == [0.4, 0.6] and rows[1]["final"] == [0.4, 0.5]
    assert rows[1]["budget"] == [0.5, 0.7] and rows[1]["macro_level"] == "elevated"
    assert rows[2]["mapping"] == "缺" and rows[2]["model"] is None
    assert rows[2]["boundary"] == 50.0 and rows[2]["distance"] == 0.0
    assert sc.boundaries(BANDS) == [35.0, 50.0, 65.0, 80.0]
    assert sc.mapped(80.0, BANDS) == sc.Bounds(0.8, 1.0)
    assert sc.mapped(34.99, BANDS) == sc.Bounds(0.0, 0.2)


def test_attribution_summary_counts_gates_tracks_confidence_and_components() -> None:
    a = snapshot_record((0.4, 0.6), (0.4, 0.6), (0.6, 0.8), score=55.0, confidence=80.0)
    b = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8), score=56.0, confidence=60.0)
    c = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8), score=58.0, confidence=60.0)
    d = snapshot_record(
        (0.4, 0.6),
        (0.4, 0.4),
        (0.6, 0.8),
        score=60.0,
        gates=[_gate("score_model", 0.6), _gate("confidence", 0.45), _gate("valuation", 0.4)],
        rules=[{"rule_id": "r1", "version": 2}, {"rule_id": "r2", "version": "a"}],
    )
    d["scores"]["components"][1]["score"] = 30.0
    records = [a, b, c, d]
    snapshots = {DAYS[i]: _snapshot(DAYS[i], record) for i, record in enumerate(records)}
    summary = sc.attribution(snapshots, BANDS)
    assert (summary["capped_days"], summary["model_days"]) == (3, 4)
    assert summary["gate_days"] == {"confidence": 2, "valuation": 1, "无额外闸门": 1}
    assert [(item["day"], item["label"]) for item in summary["changes"]] == [
        ("2026-06-01", "无额外闸门"),
        ("2026-06-02", "confidence"),
        ("2026-06-04", "valuation"),
    ]
    assert [(item["day"], item["binding"]) for item in summary["confidence"]] == [
        ("2026-06-01", False),
        ("2026-06-02", True),
        ("2026-06-04", False),
    ]
    assert summary["confidence"][1]["score"] == 60.0
    assert summary["confidence"][1]["reasons"] == ["数据新鲜"]
    components = summary["components"]
    assert (components["first"], components["last"]) == ("2026-06-01", "2026-06-04")
    assert components["first_table"][0] == {
        "component": "trend",
        "score": 55.0,
        "weight": 25.0,
        "product": 55.0 * 25,
    }
    assert components["moves"] == [
        {"component": "valuation", "change": -200.0},
        {"component": "trend", "change": 125.0},
    ]
    assert summary["versions"] == [
        {
            "day": "2026-06-01",
            "gates": ["confidence", "score_model"],
            "rules": ["r1@1"],
            "added": ["r1@1"],
            "removed": [],
        },
        {
            "day": "2026-06-04",
            "gates": ["confidence", "score_model", "valuation"],
            "rules": ["r1@2", "r2@a"],
            "added": ["r1@2", "r2@a"],
            "removed": ["r1@1"],
        },
    ]


def test_confidence_lists_the_first_the_latest_and_the_days_it_switches() -> None:
    capped = snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8), confidence=60.0)
    snapshots = {DAYS[i]: _snapshot(DAYS[i], capped) for i in range(3)}
    summary = sc.attribution(snapshots, BANDS)
    assert [(item["day"], item["binding"]) for item in summary["confidence"]] == [
        ("2026-06-01", True),
        ("2026-06-03", True),
    ]
    assert [item["day"] for item in summary["changes"]] == ["2026-06-01"]
    assert len(summary["versions"]) == 1


def test_f0001_first_needed_is_exactly_what_the_bench_needs() -> None:
    for start in (date(2026, 5, 1), date(2026, 6, 1), date(2026, 6, 2), date(2026, 7, 1)):
        needed = sc.f0001_first_needed(start)
        later = fl.trading_days(needed, date(2026, 12, 31))[1]
        fl.overlay_exposure(QQQ[QQQ.index >= pd.Timestamp(needed)], start, LAST_PRICE)
        with pytest.raises(fl.ForwardLedgerError, match="extend the lookback"):
            fl.overlay_exposure(QQQ[QQQ.index >= pd.Timestamp(later)], start, LAST_PRICE)
    assert sc.f0001_first_needed(date(2026, 5, 1)) == date(2025, 3, 28)
    assert sc.f0001_first_needed(date(2026, 6, 2)) == date(2025, 5, 28)


# The whole table on the synthetic roots ----------------------------------------------------------


def test_the_window_and_the_inputs(result: dict[str, Any], roots: tuple[Path, Path]) -> None:
    prod, archive = roots
    days = fl.trading_days(FIRST, LAST_PRICE)
    assert result["run_id"] == "TRADING-2565-80"
    assert result["code"] == {"commit": "abc", "code_modified": False}
    assert result["window"] == {
        "start": "2026-05-01",
        "end": "2026-10-22",
        "first_held": "2026-05-05",
        "days": len(days) - 2,
        "stale_days": 8,
    }
    archive_days = [d for d in fl.trading_days(FIRST, ARCHIVE_LAST) if not GAP[0] <= d <= GAP[1]]
    prod_days = fl.trading_days(PROD_FIRST, CAPTURE)
    assert [root["path"] for root in result["roots"]] == [str(prod), str(archive)]
    assert [root["files"] for root in result["roots"]] == [len(prod_days), len(archive_days)]
    assert [root["used"] for root in result["roots"]] == [len(prod_days), len(archive_days) - 5]
    assert result["roots"][1]["other_days"] == ["decision_snapshot_2026-07-04.json"]
    assert result["gaps"] == [{"first": "2026-08-03", "last": "2026-08-14", "days": 10}]
    assert result["unusable"] == [{"day": "2026-06-10", "root": 1, "reason": "invalid JSON"}]
    taken = {row["day"]: row["root"] for row in result["attribution"]["rows"]}
    assert [taken[day] for day in ("2026-08-31", "2026-09-01", "2026-09-04", "2026-09-08")] == [
        1,
        0,
        0,
        0,
    ]
    assert len(result["overlaps"]) == 4
    assert [item["day"] for item in result["overlaps"] if item["differ"]] == ["2026-09-02"]
    assert result["missing"]["model_risk_asset_ai_band"] == 1
    assert sum(result["missing"].values()) == 1
    assert result["identity"] == {"checked": len(days) - 11, "unchecked": 0, "mismatch": []}
    prices = result["prices"]
    assert prices["file"] == fl.PRICES_REL.format(day=CAPTURE.isoformat())
    assert (prices["first"], prices["last"], prices["cash"]) == ("2025-01-02", "2026-10-22", True)
    assert prices["f0001_first_needed"] == "2025-03-28"
    assert result["f0001_error"] is None
    assert result["config"] == {
        "file": str(DEFAULT_SCORING_RULES_CONFIG_PATH),
        "boundaries": [35.0, 50.0, 65.0, 80.0],
        "step": 0.05,
    }


def test_the_lines_on_the_synthetic_roots(result: dict[str, Any], roots: tuple[Path, Path]) -> None:
    days = fl.trading_days(FIRST, LAST_PRICE)
    usable = {d for d in days if not GAP[0] <= d <= GAP[1] and d != BAD_DAY}
    returns = [fl.day_return(QQQ, day) for day in days]
    overlay = fl.overlay_exposure(QQQ, FIRST, LAST_PRICE)
    final: list[float | None] = [None, None]
    model: list[float | None] = [None, None]
    for index in range(2, len(days)):
        source = max(k for k in range(index - 1) if days[k] in usable)
        record = _record(days[source])
        if days[source] == DIFFER_DAY:
            record = _record(days[source])  # the first root (prod) has 0.55 on this day
            record["positions"]["final_risk_asset_ai_band"]["max_position"] = 0.55
        final.append(_mid(record, "final_risk_asset_ai_band"))
        model.append(
            None if days[source] == NO_MODEL_DAY else _mid(record, "model_risk_asset_ai_band")
        )
    full = result["passes"]["full"]["zero"]
    indices = list(range(2, len(days)))
    assert full["final"]["log"] == pytest.approx(
        sum(math.log1p(_v(final[i]) * _v(returns[i])) for i in indices),
        abs=1e-12,
    )
    assert full["final"]["mean_exposure"] == pytest.approx(
        statistics.fmean(final[2:]),  # type: ignore[arg-type]
        abs=1e-12,
    )
    assert full["model"]["days"] == len(indices) - 1
    assert full["fixed"]["days"] == full["final"]["days"] == len(indices)
    assert full["fixed"]["versus_fixed"] == 0.0
    expected_f0001 = [float(overlay.loc[pd.Timestamp(days[i])]) for i in indices]
    assert full["f0001"]["mean_exposure"] == pytest.approx(statistics.fmean(expected_f0001))
    fresh = result["passes"]["fresh"]["zero"]
    assert fresh["final"]["days"] == len(indices) - 8
    assert set(result["passes"]["full"]) == {"zero", "sgov"}
    assert result["passes"]["full"]["sgov"]["final"]["days"] == len(indices)
    months = [row["month"] for row in result["monthly"]]
    assert months == ["2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10"]
    may = [i for i in indices if days[i].month == 5]
    assert result["monthly"][0]["final"] == pytest.approx(
        sum(math.log1p(_v(final[i]) * _v(returns[i])) for i in may),
        abs=1e-12,
    )
    assert result["monthly"][0]["final_exposure"] == pytest.approx(
        statistics.fmean(final[i] for i in may)  # type: ignore[misc]
    )


def test_events_and_reading_on_the_synthetic_roots(result: dict[str, Any]) -> None:
    rows = result["events"]["rows"]
    assert rows and all(abs(row["to"] - row["from"]) >= 0.05 - 1e-9 for row in rows)
    gap_rows = [row for row in rows if row["after_gap"]]
    assert [row["snapshots"][1] for row in gap_rows] == ["2026-08-17"]
    down = result["events"]["down"]
    assert down["events"] == sum(row["kind"] == "down" for row in rows)
    assert result["reading"]["a"]["holds"] in (True, False)
    assert set(result["reading"]) == {"a", "b"}
    json.dumps(result, allow_nan=False)


def test_a_price_file_too_short_for_f0001_leaves_its_lines_empty(tmp_path: Path) -> None:
    write_snapshot(tmp_path, date(2026, 9, 1), snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8)))
    write_snapshot(tmp_path, date(2026, 9, 2), snapshot_record((0.4, 0.6), (0.4, 0.6), (0.6, 0.8)))
    _write_capture(
        tmp_path, date(2026, 10, 2), date(2026, 10, 1), first=date(2025, 9, 2), cash=False
    )
    short = sc.build([tmp_path])
    assert short["prices"]["cash"] is False
    assert set(short["passes"]["full"]) == set(short["passes"]["fresh"]) == {"zero"}
    assert short["f0001_error"] is not None and "extend the lookback" in short["f0001_error"]
    assert short["prices"]["f0001_first_needed"] == "2025-07-30"
    assert short["passes"]["full"]["zero"]["f0001"]["days"] == 0
    assert short["passes"]["full"]["zero"]["final"]["days"] > 0
    report = sc.render(short)
    assert "F0001 需要的第一个日期 2025-07-30" in report
    assert "- F0001 没算出来：overlay F0001: no target is executable by" in report
    assert report.count("价格文件里没有 SGOV，现金按 SGOV 一组不印。") == 2


def test_the_table_stops_without_snapshots_or_prices(tmp_path: Path) -> None:
    with pytest.raises(sc.ScorecardError, match="no daily root"):
        sc.build([])
    with pytest.raises(sc.ScorecardError, match="no usable decision snapshot"):
        sc.build([tmp_path])
    write_snapshot(tmp_path, date(2026, 9, 1), snapshot_record((0.4, 0.6), (0.4, 0.5), (0.6, 0.8)))
    with pytest.raises(sc.ScorecardError, match="no data/raw/daily_input_capture"):
        sc.build([tmp_path])
    _write_capture(tmp_path, date(2026, 9, 2), date(2026, 9, 2))
    with pytest.raises(sc.ScorecardError, match="no held day between 2026-09-01"):
        sc.build([tmp_path])


# Report and command ------------------------------------------------------------------------------


def test_the_report_states_its_assumptions_and_is_print_only(result: dict[str, Any]) -> None:
    report = sc.render(result)
    assert report.startswith("# 日报仓位成绩单\n")
    assert report.endswith("\n\nproduction_effect=none；broker_action=none。\n")
    for text in (
        "依据 `owner_decision:TRADING-2565:2026-10-28:daily_band_scorecard`（第 6 期章程第 21 节）",
        "描述，不判；不是一致性窗口或验证窗口的读取。窗口只有约 5 个月，不能说谁更会判断。",
        "1. 日报·风险资产内 AI 区间中点：把总风险资产预算当作 100%，是前瞻账本的口径。",
        "2. 日报·总资产 AI 区间中点：非 AI 股票和现金都按现金算，所以偏低。",
        "6. 日报·股票/风险资产预算中点（只印）：假设非 AI 股票和 QQQ 同涨同跌，所以偏高；",
        "## 全窗口（陈旧天照沿用算），现金按 0",
        "## 去掉陈旧天，现金按 0",
        "### 全窗口（陈旧天照沿用算），现金按 SGOV（只印）",
        "噪声尺度 sd(e − ē) × sd(r) × √N，描述，不是检验",
        "## 跑之前写死的读法（需求第 5.1 节，这里只机械算条件）",
        "## 按月（现金按 0，对数点）",
        "## 降仓和加仓（第 1 条，中点变动 ≥ 换仓步长）",
        "## 拆因（照快照原样印，不重算评分）",
        "### 版本表（闸门集合或规则版本变了的日子）",
        "- 2026-08-03..2026-08-14，10 天",
        "- 2026-06-10（目录 2）：invalid JSON",
        "- 2026-09-02：目录 1 0.40–0.55，目录 2 0.40–0.50。",
        "| 2026-08-31 | 2 | ",
        "| 2026-09-01 | 1 | ",
        "F0001 需要的第一个日期 2025-03-28",
        "分档边界 35、50、65、80，换仓步长 0.05（`minimum_action_delta`）",
        "不符 0 份。",
    ):
        assert text in report, text
    assert "F0001 没算出来" not in report


def test_the_command_writes_both_files_once(roots: tuple[Path, Path], tmp_path: Path) -> None:
    prod, archive = roots
    out = tmp_path / "out" / "scorecard"
    outcome = CliRunner().invoke(
        sc.app,
        ["run", "--daily-root", str(prod), "--daily-root", str(archive), "--out", str(out)],
    )
    assert outcome.exit_code == 0, outcome.output
    assert sorted(path.name for path in out.iterdir()) == [sc.SUMMARY, sc.REPORT]
    summary = json.loads((out / sc.SUMMARY).read_text(encoding="utf-8"))
    assert summary["window"]["first_held"] == "2026-05-05"
    assert (out / sc.REPORT).read_text(encoding="utf-8").startswith("# 日报仓位成绩单")
    lines = outcome.output.splitlines()
    assert lines[0].startswith(
        "daily_band_scorecard: 2026-05-01..2026-10-22，第一个持有日 2026-05-05"
    )
    assert lines[-2:] == [
        f"daily_band_scorecard: 已写 {out / sc.SUMMARY}",
        f"daily_band_scorecard: 已写 {out / sc.REPORT}",
    ]


def test_the_command_stops_before_writing_when_it_cannot_read(tmp_path: Path) -> None:
    out = tmp_path / "out"
    outcome = CliRunner().invoke(
        sc.app, ["run", "--daily-root", str(tmp_path / "nope"), "--out", str(out)]
    )
    assert outcome.exit_code == 1
    assert outcome.output.startswith("daily_band_scorecard: 读不了：")
    assert not out.exists()
