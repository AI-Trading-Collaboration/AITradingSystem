"""TRADING-2569-06 acceptance tests: the judgment report (J3a) and the J1.1 cross-check.

Written by Claude and copied into the repository unchanged. All data here is synthetic; no real
ledger content appears in this file. Expected numbers were computed by hand or with exact
fractions, not with the code under test.
"""

from __future__ import annotations

import ast
import copy
import json
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_judgments import importer, report, reports, schema
from ai_trading_system.research_judgments.__main__ import app
from ai_trading_system.research_judgments.report import (
    ReportParams,
    benjamini_hochberg,
    build_report,
    classify,
    cluster_stats,
    earnings_quarter,
    load_report_params,
    one_sided_p,
    wilson,
)
from ai_trading_system.research_judgments.reports import make_report
from ai_trading_system.research_judgments.settle import SettleParams, load_settle_params
from ai_trading_system.research_judgments.settlements import settle as run_settle

KNOWN = {"TSM", "NVDA", "SMH"}
METHODS = {"guidance_base_rate", "capex_cycle", "accounting_detail", "m", "other"}
SECRET = "SECRET-STATEMENT-4711"
NOTE_SECRET = "SECRET-RATIONALE-8842"
ASOF = date(2026, 10, 16)
NOW = datetime(2026, 10, 16, 9, 0, 0, tzinfo=UTC)
EVAL_PATH = PROJECT_ROOT / "config" / "research" / "research_judgment_eval.yaml"

# Small thresholds so that a handful of events can reach every status. Production values are
# checked against the reviewed config in the first tests.
PARAMS = ReportParams(
    confidence=0.90,
    power=0.80,
    min_events=4,
    abandon_events=6,
    edge_brier=0.02,
    min_events_for_mde=4,
    min_clusters=3,
    bh_q=0.10,
    calibration_bins=(0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
)
# A tiny power makes the minimum detectable lift negative, which switches the MDE gate off so
# that the other status conditions can be tested on their own.
LOOSE = PARAMS.model_copy(update={"power": 0.01})
APPROX = {"abs": 1e-9}


# ---------- builders ----------


def R(
    id_: str = "r1",
    *,
    ticker: str = "TSM",
    deadline: str = "2026-10-15",
    prob: float = 0.65,
    revisions: tuple[float, ...] = (),
    methods: tuple[str, ...] = ("guidance_base_rate",),
    ai: str = "human",
    saw: bool | None = None,
    purpose: str = "research",
    status: str = "hit",
    event: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": id_,
        "ticker": ticker,
        "statement": SECRET,
        "prob": prob,
        "deadline": deadline,
        "kind": "基本面",
        "createdAt": "2026-09-01T12:00:00Z",
        "purpose": purpose,
        "aiMode": ai,
        "methods": list(methods),
        "status": status,
        "addenda": [{"at": "2026-09-02T12:00:00Z", "rationale": NOTE_SECRET}],
        "probRevisions": [
            {"at": f"2026-09-0{3 + i}T06:00:00Z", "prob": value}
            for i, value in enumerate(revisions)
        ],
    }
    if saw is not None:
        row["sawSystem"] = saw
    if event is not None:
        row["eventId"] = event
    row.update(extra)
    return row


def OC(id_: str, outcome: int | None = None, state: str = "settled") -> dict[str, Any]:
    return {"id": id_, "state": state, "outcome": outcome}


def rep(
    rows: list[dict[str, Any]], outs: list[dict[str, Any]], params: ReportParams = PARAMS
) -> dict[str, Any]:
    return build_report(rows, outs, params, ASOF)


def method(doc: dict[str, Any], name: str) -> dict[str, Any]:
    return next(m for m in doc["methods"] if m["method"] == name)


def series(
    spec: list[tuple[float, int]], name: str = "m", step: int = 100
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """One judgment per event, a hundred days apart (one per quarter), same ticker."""
    rows, outs = [], []
    start = date(2025, 1, 15)
    for i, (p, y) in enumerate(spec):
        rid = f"{name}{i:03d}"
        rows.append(
            R(
                rid,
                deadline=(start + timedelta(days=step * i)).isoformat(),
                prob=p,
                methods=(name,),
                status="hit" if y else "miss",
            )
        )
        outs.append(OC(rid, y))
    return rows, outs


def one_method(
    spec: list[tuple[float, int]], params: ReportParams = PARAMS, name: str = "m"
) -> dict[str, Any]:
    rows, outs = series(spec, name)
    return method(rep(rows, outs, params), name)


# T-B: four single-judgment events, quarters Q1 Q1 Q2 Q3; every number below is exact arithmetic.
TB = [(0.8, 1), (0.7, 1), (0.6, 0), (0.9, 1)]
TB_DAYS = ["2026-01-15", "2026-02-15", "2026-05-15", "2026-08-15"]


def tb_report(params: ReportParams = PARAMS) -> dict[str, Any]:
    rows = [
        R(f"b{i}", deadline=TB_DAYS[i], prob=p, methods=("m",), status="hit" if y else "miss")
        for i, (p, y) in enumerate(TB)
    ]
    return method(rep(rows, [OC(f"b{i}", y) for i, (_, y) in enumerate(TB)], params), "m")


# The real-shaped single event: three research judgments and one learning judgment.
def real_shaped() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = [
        R("a1", prob=0.65, revisions=(0.70,), status="hit"),
        R(
            "a2",
            prob=0.65,
            revisions=(0.55, 0.35),
            methods=("capex_cycle", "guidance_base_rate"),
            status="miss",
        ),
        R("a3", prob=0.70, status="miss"),
        R("lr", prob=0.75, purpose="learning", methods=("accounting_detail",), status="hit"),
    ]
    return rows, [OC("a1", 1), OC("a2", 0), OC("a3", 0)]


# ---------- configuration ----------


def test_default_report_config_is_the_reviewed_baseline() -> None:
    params = load_report_params()
    assert params == ReportParams(
        confidence=0.90,
        power=0.80,
        min_events=30,
        abandon_events=60,
        edge_brier=0.02,
        min_events_for_mde=5,
        min_clusters=3,
        bh_q=0.10,
        calibration_bins=(0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
    )
    raw = yaml.safe_load(EVAL_PATH.read_text("utf-8"))
    for key in (
        "policy_id", "version", "status", "owner", "rationale", "intended_effect",
        "validation_evidence", "review_condition",
    ):  # fmt: skip
        assert str(raw.get(key, "")).strip(), key
    assert raw["production_effect"] == "none"
    assert str(raw["version"]) != "2026-10-14"  # the J2 version; the report section is a new review
    assert "report" in str(raw["validation_evidence"]).lower()


def test_settlement_section_is_unchanged_by_the_report_section() -> None:
    assert load_settle_params() == SettleParams(
        market_timezone="America/New_York",
        market_close="16:00",
        max_calendar_gap_days=7,
        max_abs_daily_move=0.5,
    )


@pytest.mark.parametrize(
    "patch",
    [
        {"confidence": 0},
        {"confidence": 1},
        {"confidence": 1.5},
        {"power": 0},
        {"power": 1},
        {"min_events": 0},
        {"abandon_events": 29},
        {"edge_brier": 0},
        {"edge_brier": -0.01},
        {"min_events_for_mde": 1},
        {"min_clusters": 1},
        {"bh_q": 0},
        {"bh_q": 1},
        {"calibration_bins": [0.5]},
        {"calibration_bins": [0.6, 0.8, 1.0]},
        {"calibration_bins": [0.5, 0.9]},
        {"calibration_bins": [0.5, 0.7, 0.6, 1.0]},
        {"calibration_bins": [0.5, 0.5, 1.0]},
        {"surprise": 1},
    ],
)
def test_bad_report_config_is_refused(tmp_path: Path, patch: dict[str, Any]) -> None:
    raw = yaml.safe_load(EVAL_PATH.read_text("utf-8"))
    raw["report"].update(patch)
    path = tmp_path / "eval.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    with pytest.raises(ValueError):
        load_report_params(path)


def test_a_missing_report_section_is_refused(tmp_path: Path) -> None:
    raw = yaml.safe_load(EVAL_PATH.read_text("utf-8"))
    del raw["report"]
    path = tmp_path / "eval.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    with pytest.raises(ValueError):
        load_report_params(path)


def test_report_thresholds_are_not_hardcoded_in_the_code() -> None:
    source = PROJECT_ROOT / "src" / "ai_trading_system" / "research_judgments"
    allowed = {
        0,
        1,
        2,
        3,
        4,
        8,
        10,
        0.5,
    }  # formulas, 3 months a quarter, 0.5 coin, 10 places, 8 = sha8
    for name in ("report.py", "reports.py"):
        tree = ast.parse((source / name).read_text("utf-8"))
        named = {
            id(node.value)
            for node in tree.body
            if isinstance(node, ast.Assign)
            and all(isinstance(t, ast.Name) and t.id.isupper() for t in node.targets)
        }
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, int | float)
                and not isinstance(node.value, bool)
                and node.value not in allowed
                and id(node) not in named
            ):
                raise AssertionError(f"{name}:{node.lineno} hardcodes {node.value!r}")


# ---------- the small pure functions ----------


@pytest.mark.parametrize(
    ("k", "n", "confidence", "low", "high"),
    [
        (5, 10, 0.95, 0.23659309051256405, 0.763406909487436),
        (0, 10, 0.95, 0.0, 0.27753279986288915),
        (10, 10, 0.95, 0.7224672001371109, 1.0),
        (0, 1, 0.90, 0.0, 0.7301340512159457),
        (1, 1, 0.90, 0.2698659487840544, 1.0),
        (1, 2, 0.90, 0.12086631942227372, 0.8791336805777263),
        (1.5, 2, 0.90, 0.2478324531548493, 0.9646828513483241),
    ],
)
def test_wilson_interval_matches_hand_values(
    k: float, n: int, confidence: float, low: float, high: float
) -> None:
    got = wilson(k, n, confidence)
    assert got[0] == pytest.approx(low, abs=1e-9) and got[1] == pytest.approx(high, abs=1e-9)
    assert 0.0 <= got[0] <= got[1] <= 1.0


def test_wilson_bounds_are_exactly_zero_and_one_when_rounding_would_leave_the_unit_interval() -> (
    None
):
    # without clamping these come out as -2.8e-17 and 1.0000000000000002
    assert wilson(0, 5, 0.80)[0] == 0.0
    assert wilson(9, 9, 0.95)[1] == 1.0


def test_wilson_is_two_sided_at_the_given_confidence() -> None:
    narrow, wide = wilson(3, 10, 0.80), wilson(3, 10, 0.99)
    assert wide[0] < narrow[0] < 0.3 < narrow[1] < wide[1]


def test_cluster_stats_two_clusters_by_hand() -> None:
    got = cluster_stats([1.0, 2.0, 3.0, 4.0], ["a", "a", "b", "b"])
    # u_a = -2, u_b = 2, G = 2: V = 2/1 * (4 + 4) / 16 = 1
    assert got["mean"] == pytest.approx(2.5) and got["se"] == pytest.approx(1.0)
    assert got["clusters"] == 2


def test_cluster_stats_one_cluster_per_value() -> None:
    got = cluster_stats([1.0, 3.0], ["a", "b"])
    assert got["mean"] == pytest.approx(2.0) and got["se"] == pytest.approx(1.0)  # 2 * 2 / 4


def test_cluster_stats_unequal_clusters_use_the_small_cluster_factor() -> None:
    got = cluster_stats([0.0, 0.0, 3.0], ["a", "a", "b"])
    # mean 1; u_a = -2, u_b = 2, G = 2: V = 2 * 8 / 9
    assert got["se"] == pytest.approx((16 / 9) ** 0.5)
    three = cluster_stats([1.0, 2.0, 6.0], ["a", "b", "c"])
    # mean 3; u = -2, -1, 3; G = 3: V = 1.5 * 14 / 9
    assert three["se"] == pytest.approx((21 / 9) ** 0.5)


def test_cluster_stats_without_spread_has_zero_error_and_one_cluster_has_none() -> None:
    assert cluster_stats([2.0, 2.0, 2.0], ["a", "b", "c"])["se"] == 0.0
    assert cluster_stats([1.0, 2.0, 3.0], ["a", "a", "a"])["se"] is None
    assert cluster_stats([5.0], ["a"])["se"] is None


def test_one_sided_p_values() -> None:
    assert one_sided_p(0.0, 1.0) == pytest.approx(0.5)
    assert one_sided_p(1.6448536269514722, 1.0) == pytest.approx(0.05, abs=1e-9)
    assert one_sided_p(-1.0, 1.0) == pytest.approx(0.8413447460685429, abs=1e-9)
    assert one_sided_p(0.3, 0.0) == 0.0
    assert one_sided_p(-0.3, 0.0) == 1.0
    assert one_sided_p(0.0, 0.0) == 1.0
    assert one_sided_p(0.3, None) is None


@pytest.mark.parametrize(
    ("day", "quarter"),
    [
        ("2026-01-01", "2026Q1"),
        ("2026-03-31", "2026Q1"),
        ("2026-04-01", "2026Q2"),
        ("2026-06-30", "2026Q2"),
        ("2026-07-01", "2026Q3"),
        ("2026-10-15", "2026Q4"),
        ("2025-12-31", "2025Q4"),
    ],
)
def test_earnings_quarter_is_the_calendar_quarter_of_the_event(day: str, quarter: str) -> None:
    assert earnings_quarter(day) == quarter


def test_benjamini_hochberg_is_a_step_up_procedure() -> None:
    p = {"a": 0.001, "b": 0.02, "c": 0.04, "d": 0.5, "e": None}
    assert benjamini_hochberg(p, 0.10) == {"a", "b", "c"}
    # rank 1 misses its own threshold (0.06 > 0.05) but rank 2 meets its own (0.07 <= 0.10),
    # so both pass
    assert benjamini_hochberg({"a": 0.06, "b": 0.07}, 0.10) == {"a", "b"}
    assert benjamini_hochberg({"a": 0.2, "b": 0.012}, 0.10) == {"b"}
    assert benjamini_hochberg({"a": 0.2, "b": 0.3}, 0.10) == set()
    assert benjamini_hochberg({"a": None, "b": None}, 0.10) == set()
    assert benjamini_hochberg({}, 0.10) == set()


def test_benjamini_hochberg_counts_groups_without_a_p_value() -> None:
    # 0.011 passes alone (<= 0.10) and with one other group (<= 0.05) but not among ten (<= 0.01)
    assert benjamini_hochberg({"a": 0.011}, 0.10) == {"a"}
    assert benjamini_hochberg({"a": 0.011, "b": None}, 0.10) == {"a"}
    many: dict[str, float | None] = {f"x{i}": None for i in range(9)}
    many["a"] = 0.011
    assert benjamini_hochberg(many, 0.10) == set()


def test_benjamini_hochberg_ties_and_q() -> None:
    assert benjamini_hochberg({"a": 0.04, "b": 0.04}, 0.10) == {"a", "b"}
    assert benjamini_hochberg({"a": 0.04}, 0.03) == set()
    assert benjamini_hochberg({"a": 0.03}, 0.03) == {"a"}  # inclusive


GOOD = {
    "n_events": 40,
    "mde": 0.01,
    "lift_mean": 0.10,
    "lift_lower": 0.05,
    "lift_upper": 0.15,
    "lift_first_half": 0.10,
    "lift_second_half": 0.10,
}


@pytest.mark.parametrize(
    ("patch", "bh", "expected"),
    [
        ({}, True, ("promising", "edge_reached")),
        ({}, False, ("insufficient", "keep_collecting")),
        ({"n_events": 3}, True, ("insufficient", "events_below_minimum")),
        ({"n_events": 4}, True, ("promising", "edge_reached")),
        ({"mde": None}, True, ("insufficient", "mde_unavailable")),
        ({"mde": 0.0200001}, True, ("insufficient", "mde_above_edge")),
        ({"mde": 0.02}, True, ("promising", "edge_reached")),
        ({"mde": -0.5}, True, ("promising", "edge_reached")),
        ({"lift_mean": 0.0199999}, True, ("insufficient", "keep_collecting")),
        ({"lift_mean": 0.02}, True, ("promising", "edge_reached")),
        ({"lift_lower": 0.0}, True, ("insufficient", "keep_collecting")),
        ({"lift_lower": 1e-9}, True, ("promising", "edge_reached")),
        ({"lift_first_half": 0.0}, True, ("insufficient", "keep_collecting")),
        ({"lift_second_half": 0.0}, True, ("insufficient", "keep_collecting")),
        ({"lift_first_half": None}, True, ("insufficient", "keep_collecting")),
        (
            {"lift_first_half": -0.01, "lift_second_half": 0.3},
            True,
            ("insufficient", "keep_collecting"),
        ),
        (
            {"n_events": 6, "lift_mean": 0.01, "lift_upper": 0.0199999},
            True,
            ("abandon", "edge_excluded"),
        ),
        (
            {"n_events": 6, "lift_mean": 0.01, "lift_upper": 0.02},
            True,
            ("insufficient", "keep_collecting"),
        ),
        (
            {"n_events": 5, "lift_mean": 0.01, "lift_upper": 0.01},
            True,
            ("insufficient", "keep_collecting"),
        ),
        (
            {"n_events": 6, "lift_mean": 0.0, "lift_upper": 0.01, "lift_lower": -0.01},
            False,
            ("abandon", "edge_excluded"),
        ),
        (
            {"n_events": 6, "lift_mean": -0.05, "lift_upper": 0.0, "mde": 0.03},
            True,
            ("insufficient", "mde_above_edge"),
        ),
    ],
)
def test_status_table(patch: dict[str, Any], bh: bool, expected: tuple[str, str]) -> None:
    assert classify({**GOOD, **patch}, PARAMS, bh) == expected


def test_status_thresholds_come_from_the_parameters() -> None:
    strict = PARAMS.model_copy(update={"min_events": 41, "abandon_events": 60})
    assert classify(GOOD, strict, True) == ("insufficient", "events_below_minimum")
    wide = PARAMS.model_copy(update={"edge_brier": 0.2, "abandon_events": 100})
    assert classify({**GOOD, "mde": 0.15}, wide, True)[1] == "keep_collecting"  # mean 0.10 < 0.2
    assert classify({**GOOD, "mde": 0.25}, wide, True)[1] == "mde_above_edge"
    late = PARAMS.model_copy(update={"abandon_events": 7})
    sample = {**GOOD, "n_events": 6, "lift_mean": 0.0, "lift_upper": 0.01, "lift_lower": -0.01}
    assert classify(sample, PARAMS, True)[0] == "abandon"
    assert classify(sample, late, True)[0] == "insufficient"


# ---------- the report: counts, events, scores ----------


def test_real_shaped_event_scores_by_hand() -> None:
    rows, outs = real_shaped()
    doc = rep(rows, outs)
    assert doc["format"] == "judgment_report.v1" and doc["asof"] == "2026-10-16"
    assert [m["method"] for m in doc["methods"]] == ["capex_cycle", "guidance_base_rate"]
    g = method(doc, "guidance_base_rate")
    # a1: scored 0.7, hit -> 0.09 / original 0.65 -> 0.1225; a2: 0.35, miss -> 0.1225 / 0.4225;
    # a3: 0.7, miss -> 0.49 both. One event: the mean of the three.
    assert (g["n_judgments"], g["n_events"], g["n_clusters"]) == (3, 1, 1)
    assert g["brier_scored"] == pytest.approx(0.7025 / 3, **APPROX)
    assert g["brier_original"] == pytest.approx(0.345, **APPROX)
    assert g["brier_coin"] == 0.25
    assert g["hit_rate"] == pytest.approx(1 / 3, **APPROX)
    assert g["hit_wilson_low"] == pytest.approx(0.034237092389505064, **APPROX)
    assert g["hit_wilson_high"] == pytest.approx(0.8758075913491434, **APPROX)
    assert g["update_gain"] == pytest.approx(0.345 - 0.7025 / 3, **APPROX)
    assert (g["n_updated"], g["n_flipped"]) == (1, 1)
    assert g["brier_baseline"] is None and g["lift_mean"] is None and g["mde"] is None
    assert g["lift_se"] is None and g["lift_lower"] is None and g["lift_p"] is None
    assert g["lift_coin_mean"] == pytest.approx(0.25 - 0.7025 / 3, **APPROX)
    assert (g["status"], g["reason"]) == ("insufficient", "events_below_minimum")
    c = method(doc, "capex_cycle")
    assert (c["n_judgments"], c["n_events"]) == (1, 1)
    assert c["brier_scored"] == pytest.approx(0.1225, **APPROX)
    assert c["brier_original"] == pytest.approx(0.4225, **APPROX)
    assert c["update_gain"] == pytest.approx(0.3, **APPROX)
    assert c["hit_rate"] == 0.0 and c["hit_wilson_low"] == 0.0
    assert c["hit_wilson_high"] == pytest.approx(0.7301340512159457, **APPROX)
    assert doc["bh"] == {"m": 2, "q": 0.10}


def test_learning_judgments_have_their_own_section_and_no_method_group() -> None:
    rows, outs = real_shaped()
    doc = rep(rows, outs)
    assert "accounting_detail" not in [m["method"] for m in doc["methods"]]
    lr = doc["learning"]
    assert (lr["n_judgments"], lr["n_events"], lr["hit_rate"]) == (1, 1, 1.0)
    assert lr["brier_scored"] == pytest.approx(0.0625, **APPROX)
    assert lr["hit_wilson_low"] == pytest.approx(0.2698659487840544, **APPROX)
    assert "status" not in lr and "reason" not in lr
    research = doc["research"]
    assert research["n_judgments"] == 3 and research["n_events"] == 1
    assert research["brier_scored"] == pytest.approx(0.7025 / 3, **APPROX)
    everything = doc["all_settled"]
    assert everything["n_judgments"] == 4 and everything["n_events"] == 1
    assert everything["brier_scored"] == pytest.approx(0.19125, **APPROX)
    assert everything["brier_original"] == pytest.approx(0.274375, **APPROX)
    assert everything["hit_rate"] == pytest.approx(0.5, **APPROX)
    assert everything["update_gain"] == pytest.approx(0.083125, **APPROX)
    assert doc["counts"] == {"settled": 3, "pending": 0, "void": 0, "data_gap": 0}


def test_learning_result_comes_from_the_ledger_and_only_when_settled() -> None:
    rows, outs = real_shaped()
    open_row = R("lo", purpose="learning", methods=("other",), status="open")
    void_row = R("lv", purpose="learning", methods=("other",), status="void")
    miss_row = R("lm", purpose="learning", methods=("other",), status="miss")
    doc = rep(rows + [open_row, void_row, miss_row], outs)
    assert doc["learning"]["n_judgments"] == 2  # the hit and the miss
    assert doc["learning"]["hit_rate"] == pytest.approx(0.5)
    empty = rep([R("x", status="open", purpose="learning", methods=("other",))], [])
    assert empty["learning"] is None and empty["research"] is None and empty["all_settled"] is None
    assert empty["methods"] == [] and all(row["n"] == 0 for row in empty["calibration"])


def test_only_settled_research_judgments_are_scored_and_the_rest_are_counted() -> None:
    rows = [R(f"s{i}", deadline=f"2026-0{i + 1}-15", methods=("m",)) for i in range(7)]
    outs = [
        OC("s0", 1),
        OC("s1", 0),
        OC("s2", None, "pending"),
        OC("s3", None, "pending"),
        OC("s4", None, "void"),
        OC("s5", None, "data_gap"),
        OC("s6", None, "data_gap"),
    ]
    doc = rep(rows, outs)
    assert doc["counts"] == {"settled": 2, "pending": 2, "void": 1, "data_gap": 2}
    assert method(doc, "m")["n_judgments"] == 2
    assert doc["research"]["n_judgments"] == 2


def test_judgments_of_one_event_are_one_sample() -> None:
    rows = [R("p", methods=("m",)), R("q", methods=("m",), status="miss")]
    doc = rep(rows, [OC("p", 1), OC("q", 0)])
    g = method(doc, "m")
    assert (g["n_judgments"], g["n_events"], g["n_clusters"]) == (2, 1, 1)  # same ticker + deadline


@pytest.mark.parametrize(
    ("second", "events"),
    [
        ({"ticker": "NVDA"}, 2),  # other ticker, same deadline
        ({"deadline": "2026-10-16"}, 2),  # same ticker, other deadline
        ({"event": "E1"}, 2),  # explicit id against the derived one
    ],
)
def test_events_are_split_by_ticker_deadline_or_id(second: dict[str, Any], events: int) -> None:
    rows = [R("p", methods=("m",)), R("q", methods=("m",), **second)]
    g = method(rep(rows, [OC("p", 1), OC("q", 1)]), "m")
    assert g["n_events"] == events


def test_an_event_id_merges_across_tickers_and_deadlines_and_dates_the_event_by_its_first() -> None:
    rows = [
        R("p", ticker="TSM", deadline="2026-03-30", event="E1", methods=("m",)),
        R("q", ticker="NVDA", deadline="2026-04-02", event="E1", methods=("m",)),
        R("r", ticker="TSM", deadline="2026-05-10", event="E2", methods=("m",)),
    ]
    g = method(rep(rows, [OC("p", 1), OC("q", 1), OC("r", 1)]), "m")
    assert (g["n_judgments"], g["n_events"]) == (3, 2)
    # E1 is dated by its first deadline (2026-03-30, Q1), E2 is Q2; by its last E1 would be Q2 too
    assert g["n_clusters"] == 2


def test_event_scores_are_means_of_judgment_scores_not_a_pooled_mean() -> None:
    rows = [
        R("p", prob=0.9, methods=("m",)),
        R("q", prob=0.7, methods=("m",), status="miss"),
        R("r", prob=0.8, deadline="2026-11-20", methods=("m",)),
    ]
    g = method(rep(rows, [OC("p", 1), OC("q", 0), OC("r", 1)]), "m")
    # event 1: (0.01 + 0.49) / 2 = 0.25; event 2: 0.04. Mean 0.145 (a pooled mean would be 0.18)
    assert g["brier_scored"] == pytest.approx(0.145, **APPROX)
    # event hit rates 0.5 and 1: 1.5 events out of 2
    assert g["hit_rate"] == pytest.approx(0.75, **APPROX)
    assert g["hit_wilson_low"] == pytest.approx(0.2478324531548493, **APPROX)
    assert g["hit_wilson_high"] == pytest.approx(0.9646828513483241, **APPROX)


def test_scoring_uses_the_last_revision_and_keeps_the_original_for_comparison() -> None:
    rows = [R("p", prob=0.6, revisions=(0.7, 0.8), methods=("m",))]
    g = method(rep(rows, [OC("p", 1)]), "m")
    assert g["brier_scored"] == pytest.approx(0.04, **APPROX)
    assert g["brier_original"] == pytest.approx(0.16, **APPROX)
    assert g["update_gain"] == pytest.approx(0.12, **APPROX)


def test_update_gain_flips_and_updated_events() -> None:
    spec = [
        ("u0", 0.65, (0.70,), 1),  # up, same side
        ("u1", 0.65, (0.35,), 0),  # flipped
        ("u2", 0.40, (0.45,), 0),  # up, both below 0.5
        ("u3", 0.50, (0.60,), 1),  # from exactly 0.5: not a flip
        ("u4", 0.45, (0.55,), 1),  # flipped
        ("u5", 0.70, (), 1),  # untouched
        ("u6", 0.70, (0.70,), 1),  # revised to the same value: not an update
    ]
    rows = [
        R(rid, deadline=f"2026-0{i + 1}-10", prob=p, revisions=rev, methods=("m",))
        for i, (rid, p, rev, _) in enumerate(spec)
    ]
    g = method(rep(rows, [OC(rid, y) for rid, _, _, y in spec]), "m")
    assert g["n_flipped"] == 2
    assert g["n_updated"] == 5
    original = [0.65, 0.65, 0.40, 0.50, 0.45, 0.70, 0.70]
    scored = [0.70, 0.35, 0.45, 0.60, 0.55, 0.70, 0.70]
    ys = [y for *_, y in spec]
    gain = sum((q - y) ** 2 - (p - y) ** 2 for q, p, y in zip(original, scored, ys, strict=True))
    assert g["update_gain"] == pytest.approx(gain / 7, **APPROX)


def test_no_revision_means_zero_gain_and_no_update() -> None:
    g = method(rep([R("p", methods=("m",))], [OC("p", 1)]), "m")
    assert (g["update_gain"], g["n_updated"], g["n_flipped"]) == (0.0, 0, 0)
    assert g["brier_scored"] == g["brier_original"]


# ---------- the lift over the base rate ----------


def test_four_events_by_hand() -> None:
    g = tb_report()
    assert (g["n_judgments"], g["n_events"], g["n_clusters"]) == (4, 4, 3)
    assert g["hit_rate"] == pytest.approx(0.75, **APPROX)
    assert g["hit_wilson_low"] == pytest.approx(0.3561680085985065, **APPROX)
    assert g["hit_wilson_high"] == pytest.approx(0.9420926788001412, **APPROX)
    assert g["brier_scored"] == pytest.approx(0.125, **APPROX)
    assert g["brier_original"] == pytest.approx(0.125, **APPROX)
    # leave-one-event-out base rates 2/3, 2/3, 1, 2/3 -> (1/9, 1/9, 1, 1/9) -> 1/3
    assert g["brier_baseline"] == pytest.approx(1 / 3, **APPROX)
    assert g["lift_mean"] == pytest.approx(0.20833333333333334, **APPROX)
    assert g["lift_first_half"] == pytest.approx(0.04611111111111111, **APPROX)
    assert g["lift_second_half"] == pytest.approx(0.3705555555555556, **APPROX)
    assert g["lift_coin_mean"] == pytest.approx(0.125, **APPROX)
    assert g["lift_se"] == pytest.approx(0.16856848948765055, **APPROX)
    assert g["lift_lower"] == pytest.approx(-0.007695878271053808, **APPROX)
    assert g["lift_upper"] == pytest.approx(0.4243625449377205, **APPROX)
    assert g["lift_p"] == pytest.approx(0.10824837886947503, **APPROX)
    assert g["mde"] == pytest.approx(0.3579000316685064, **APPROX)
    assert (g["status"], g["reason"]) == ("insufficient", "mde_above_edge")


def test_the_leave_one_out_base_rate_never_uses_the_event_itself() -> None:
    # two events, y = 1 and 0: each is judged against the other's result
    rows = [R("p", deadline="2026-01-10", prob=0.5, methods=("m",)),
            R("q", deadline="2026-04-10", prob=0.5, methods=("m",), status="miss")]  # fmt: skip
    g = method(rep(rows, [OC("p", 1), OC("q", 0)]), "m")
    # base rate for p is 0 (q's result): (0 - 1)^2 = 1; for q it is 1: also 1. Mean 1.
    assert g["brier_baseline"] == pytest.approx(1.0, **APPROX)
    assert g["lift_mean"] == pytest.approx(1.0 - 0.25, **APPROX)


def test_one_event_has_no_base_rate_and_no_lift() -> None:
    g = method(rep([R("p", methods=("m",))], [OC("p", 1)]), "m")
    assert g["brier_baseline"] is None and g["lift_mean"] is None
    assert g["lift_first_half"] is None and g["lift_second_half"] is None


def test_one_cluster_gives_a_lift_but_no_error_bounds() -> None:
    rows = [R("p", deadline="2026-01-10", prob=0.8, methods=("m",)),
            R("q", deadline="2026-02-10", prob=0.7, methods=("m",), status="miss"),
            R("r", deadline="2026-03-10", prob=0.9, methods=("m",))]  # fmt: skip
    g = method(rep(rows, [OC("p", 1), OC("q", 0), OC("r", 1)]), "m")
    assert g["n_clusters"] == 1 and g["lift_mean"] is not None
    for key in ("lift_se", "lift_lower", "lift_upper", "lift_p", "mde"):
        assert g[key] is None, key


@pytest.mark.parametrize(
    ("n_events", "n_clusters", "has_mde"),
    [(4, 3, True), (3, 3, False), (4, 2, False), (6, 6, True)],
)
def test_mde_needs_enough_events_and_clusters(
    n_events: int, n_clusters: int, has_mde: bool
) -> None:
    days = [date(2025, 1, 10) + timedelta(days=100 * (i % n_clusters)) for i in range(n_events)]
    rows, outs = [], []
    for i, day in enumerate(days):
        y = 1 if i % 3 else 0
        rid = f"e{i}"
        rows.append(R(rid, ticker=f"T{i}", deadline=day.isoformat(), prob=0.7, methods=("m",)))
        outs.append(OC(rid, y))
    g = method(rep(rows, outs), "m")
    assert g["n_clusters"] == n_clusters
    assert (g["mde"] is not None) is has_mde


def test_mde_scales_with_the_error_and_the_power() -> None:
    low, high = tb_report(PARAMS), tb_report(PARAMS.model_copy(update={"power": 0.95}))
    assert low["mde"] == pytest.approx((1.2815515655446004 + 0.8416212335729143) * low["lift_se"])
    assert high["mde"] == pytest.approx((1.2815515655446004 + 1.6448536269514722) * high["lift_se"])
    wide = tb_report(PARAMS.model_copy(update={"confidence": 0.95}))
    assert wide["lift_lower"] == pytest.approx(
        wide["lift_mean"] - 1.6448536269514722 * wide["lift_se"]
    )
    assert wide["lift_upper"] == pytest.approx(
        wide["lift_mean"] + 1.6448536269514722 * wide["lift_se"]
    )


def test_lift_over_a_coin_flip_is_reported_separately() -> None:
    g = tb_report()
    assert g["lift_coin_mean"] != g["lift_mean"]
    assert g["brier_coin"] == 0.25


# ---------- statuses end to end ----------

INFORMATIVE = [(0.9, 1), (0.1, 0), (0.8, 1), (0.2, 0), (0.85, 1), (0.15, 0), (0.7, 1), (0.3, 0)]


def test_a_clearly_informative_method_is_promising() -> None:
    g = one_method(INFORMATIVE, LOOSE)
    assert g["n_events"] == 8
    assert g["lift_mean"] == pytest.approx(0.2859056122, abs=1e-9)
    assert g["lift_lower"] > 0.27
    assert (g["status"], g["reason"]) == ("promising", "edge_reached")


def test_the_same_method_is_insufficient_when_the_mde_gate_is_on() -> None:
    g = one_method(INFORMATIVE, PARAMS)
    assert g["mde"] > PARAMS.edge_brier
    assert (g["status"], g["reason"]) == ("insufficient", "mde_above_edge")


def test_a_method_that_only_improved_late_is_not_promising() -> None:
    spec = [(0.4, 1), (0.6, 0)] * 10 + [(0.95, 1), (0.05, 0)] * 10
    g = one_method(spec, LOOSE)
    assert g["n_events"] == 40
    assert g["lift_mean"] == pytest.approx(0.08173487836949375, abs=1e-9)
    assert g["lift_lower"] == pytest.approx(0.04505314646183791, abs=1e-9)
    assert g["lift_first_half"] == pytest.approx(-0.09701512163050624, abs=1e-9)
    assert g["lift_second_half"] == pytest.approx(0.26048487836949374, abs=1e-9)
    assert (g["status"], g["reason"]) == ("insufficient", "keep_collecting")


def test_a_no_skill_forecaster_with_a_positive_noise_lift_is_not_promising() -> None:
    # always 0.5 on balanced results: the leave-one-out base rate is slightly worse than a
    # coin, so the lift is positive and exact, but far below the edge worth pursuing
    spec = [(0.5, 1), (0.5, 0)] * 20
    g = one_method(spec, LOOSE.model_copy(update={"abandon_events": 100}))
    assert 0 < g["lift_mean"] < 0.02 and g["lift_lower"] > 0
    assert (g["status"], g["reason"]) == ("insufficient", "keep_collecting")
    # with enough events the same small lift is excluded from the edge: abandoned
    assert one_method(spec, LOOSE)["status"] == "abandon"


def test_a_method_whose_lift_is_excluded_from_the_edge_is_abandoned() -> None:
    wide = PARAMS.model_copy(update={"edge_brier": 0.2})
    g = one_method([(0.5, 1), (0.5, 0)] * 4, wide)
    assert g["n_events"] == 8 and g["n_events"] >= wide.abandon_events
    assert g["lift_upper"] < 0.2
    assert (g["status"], g["reason"]) == ("abandon", "edge_excluded")
    short = one_method([(0.5, 1), (0.5, 0)] * 2, wide)  # 4 events: not enough to abandon
    assert short["status"] == "insufficient"


def test_below_the_event_minimum_is_always_insufficient_whatever_the_lift() -> None:
    g = one_method(INFORMATIVE[:3], LOOSE)
    assert (g["status"], g["reason"]) == ("insufficient", "events_below_minimum")


def test_the_false_discovery_filter_counts_every_method_group() -> None:
    spec = [(0.6, 0), (0.4, 0), (0.1, 0), (0.3, 0), (0.1, 0), (0.6, 1), (0.3, 0), (0.8, 1),
            (0.7, 1), (0.4, 0), (0.9, 1), (0.1, 0)]  # fmt: skip
    rows, outs = series(spec, "m")
    alone = method(rep(rows, outs, LOOSE), "m")
    assert alone["lift_p"] == pytest.approx(0.007559628584728584, abs=1e-9)
    assert alone["lift_mean"] >= LOOSE.edge_brier and alone["lift_lower"] > 0
    assert (alone["status"], alone["reason"]) == ("promising", "edge_reached")
    more_rows, more_outs = list(rows), list(outs)
    for i in range(20):  # twenty other ideas with one judgment each: they have no p-value
        more_rows.append(R(f"z{i}", deadline="2026-02-01", methods=(f"z{i:02d}",), ticker="NVDA"))
        more_outs.append(OC(f"z{i}", 1))
    crowded = rep(more_rows, more_outs, LOOSE)
    assert crowded["bh"] == {"m": 21, "q": 0.10}
    tested = method(crowded, "m")
    assert tested["lift_p"] == alone["lift_p"]  # the p-value is unchanged; the bar moved
    assert (tested["status"], tested["reason"]) == ("insufficient", "keep_collecting")
    loose_q = LOOSE.model_copy(update={"bh_q": 0.5})
    assert method(rep(more_rows, more_outs, loose_q), "m")["status"] == "promising"


# ---------- groups and calibration ----------


def test_a_judgment_belongs_to_every_one_of_its_methods_and_groups_are_sorted() -> None:
    rows = [
        R("p", methods=("m", "capex_cycle", "guidance_base_rate")),
        R("q", deadline="2026-11-02", methods=("m",)),
    ]
    doc = rep(rows, [OC("p", 1), OC("q", 0)])
    assert [g["method"] for g in doc["methods"]] == ["capex_cycle", "guidance_base_rate", "m"]
    assert [g["n_judgments"] for g in doc["methods"]] == [1, 1, 2]
    assert doc["research"]["n_judgments"] == 2  # each judgment once in the pooled research row


def test_descriptive_groups_give_numbers_but_never_a_status() -> None:
    rows = [
        R("p", ticker="TSM", ai="human", saw=True, methods=("m",)),
        R("q", ticker="TSM", ai="assisted", saw=False, deadline="2026-11-01", methods=("m",)),
        R("r", ticker="NVDA", ai="assisted", deadline="2026-11-02", methods=("m",)),
    ]
    doc = rep(rows, [OC("p", 1), OC("q", 0), OC("r", 1)])
    assert [g["ticker"] for g in doc["by_ticker"]] == ["NVDA", "TSM"]
    assert [g["aiMode"] for g in doc["by_ai_mode"]] == ["assisted", "human"]
    assert [g["sawSystem"] for g in doc["by_saw_system"]] == ["no", "unknown", "yes"]
    for key in ("by_ticker", "by_ai_mode", "by_saw_system"):
        for g in doc[key]:
            assert "status" not in g and "reason" not in g and "calibration" not in g
            assert g["n_judgments"] >= 1 and "brier_scored" in g and "brier_original" in g
    tsm = next(g for g in doc["by_ticker"] if g["ticker"] == "TSM")
    assert tsm["n_judgments"] == 2 and tsm["n_events"] == 2


def edges(table: list[dict[str, Any]]) -> list[tuple[float, float]]:
    return [(row["low"], row["high"]) for row in table]


def test_calibration_folds_low_probabilities_to_the_opposite_statement() -> None:
    rows = [
        R(
            "a", deadline="2026-01-10", prob=0.35, methods=("m",)
        ),  # 35% that X; X false -> 65% right
        R(
            "b", deadline="2026-02-10", prob=0.20, methods=("m",), status="hit"
        ),  # 80% against, wrong
        R("c", deadline="2026-03-10", prob=0.50, methods=("m",)),  # exactly 0.5: not folded
        R("d", deadline="2026-04-10", prob=0.60, methods=("m",)),  # lower edge belongs to the bin
        R("e", deadline="2026-05-10", prob=0.95, methods=("m",)),
        R(
            "f", deadline="2026-06-10", prob=0.05, methods=("m",), status="hit"
        ),  # 95% against, wrong
    ]
    ys = {"a": 0, "b": 1, "c": 1, "d": 1, "e": 1, "f": 1}
    doc = rep(rows, [OC(k, v) for k, v in ys.items()])
    table = doc["calibration"]
    assert edges(table) == [(0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0)]
    assert [row["n"] for row in table] == [1, 2, 0, 1, 2]
    first, second, third, fourth, fifth = table
    assert first["mean_p"] == pytest.approx(0.5) and first["hit_rate"] == 1.0
    assert second["mean_p"] == pytest.approx(0.625, **APPROX) and second["hit_rate"] == 1.0
    assert second["wilson_low"] == pytest.approx(0.4250306090, abs=1e-9)
    assert second["wilson_high"] == 1.0
    assert third["mean_p"] is None and third["hit_rate"] is None and third["wilson_low"] is None
    assert fourth["mean_p"] == pytest.approx(0.8, **APPROX) and fourth["hit_rate"] == 0.0
    assert fifth["mean_p"] == pytest.approx(0.95, **APPROX) and fifth["hit_rate"] == 0.5
    assert fifth["wilson_low"] == pytest.approx(0.12086631942227372, **APPROX)
    assert fifth["wilson_high"] == pytest.approx(0.8791336805777263, **APPROX)
    assert method(doc, "m")["calibration"] == table


def test_calibration_uses_the_scored_probability_and_each_judgment_once() -> None:
    rows = [R("a", prob=0.55, revisions=(0.95,), methods=("m",)),
            R("b", prob=0.65, methods=("m",), status="miss")]  # fmt: skip
    doc = rep(rows, [OC("a", 1), OC("b", 0)])  # same event: still two table entries
    assert [row["n"] for row in doc["calibration"]] == [0, 1, 0, 0, 1]


def test_calibration_bins_come_from_the_parameters() -> None:
    params = PARAMS.model_copy(update={"calibration_bins": (0.5, 0.75, 1.0)})
    rows = [
        R("a", prob=0.74, methods=("m",)),
        R("b", prob=0.75, deadline="2026-11-01", methods=("m",)),
    ]
    table = rep(rows, [OC("a", 1), OC("b", 1)], params)["calibration"]
    assert edges(table) == [(0.5, 0.75), (0.75, 1.0)] and [row["n"] for row in table] == [1, 1]


def test_report_has_no_judgment_text_and_does_not_touch_its_inputs() -> None:
    rows, outs = real_shaped()
    before = copy.deepcopy((rows, outs))
    doc = rep(rows, outs)
    assert (rows, outs) == before
    blob = json.dumps(doc, ensure_ascii=False)
    assert SECRET not in blob and NOTE_SECRET not in blob
    assert json.dumps(rep(rows, outs), sort_keys=True) == json.dumps(doc, sort_keys=True)


def test_input_order_does_not_matter() -> None:
    spec = [(0.6, 0), (0.4, 0), (0.1, 0), (0.3, 0), (0.6, 1), (0.8, 1), (0.7, 1), (0.9, 1)]
    rows, outs = series(spec, "m")
    base = json.dumps(rep(rows, outs, LOOSE), sort_keys=True)
    rng = random.Random(3)
    for _ in range(5):
        rng.shuffle(rows)
        rng.shuffle(outs)
        assert json.dumps(rep(rows, outs, LOOSE), sort_keys=True) == base


def test_the_two_halves_follow_event_dates_not_input_order() -> None:
    spec = [(0.9, 0), (0.1, 1), (0.9, 0), (0.1, 1)] + [(0.9, 1), (0.1, 0)] * 4
    rows, outs = series(spec, "m")
    rows.reverse()
    g = method(rep(rows, outs, LOOSE), "m")
    assert g["lift_first_half"] < 0 < g["lift_second_half"]


# ---------- the J1.1 cross-check in the importer checks ----------


def J(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": "a1",
        "ticker": "TSM",
        "statement": SECRET,
        "prob": 0.65,
        "deadline": "2026-10-15",
        "kind": "基本面",
        "createdAt": "2026-10-01T06:16:08.523Z",
        "purpose": "research",
        "aiMode": "human",
        "methods": ["guidance_base_rate"],
        "status": "open",
        "addenda": [],
    }
    base.update(over)
    return base


def check(rows: list[dict[str, Any]], baseline: dict[str, Any] | None = None) -> list[str]:
    doc = {
        "format": "judgments_export.v1",
        "exported_at": "2026-10-16T02:45:14Z",
        "judgments": rows,
    }
    issues = schema.check_export(doc, known_tickers=KNOWN, method_ids=METHODS, baseline=baseline)
    return sorted(i.code for i in issues)


def with_addendum(**add: Any) -> dict[str, Any]:
    entry = {"at": "2026-10-05T06:26:24.047Z", "rationale": NOTE_SECRET, **add}
    return J(addenda=[entry])


REV = [{"at": "2026-10-05T06:26:24.047Z", "prob": 0.7}]


def test_an_addendum_probability_with_a_matching_revision_is_accepted() -> None:
    assert check([with_addendum(prob=0.7, probNote="note") | {"probRevisions": REV}]) == []


def test_an_addendum_probability_without_a_revision_is_refused() -> None:
    assert check([with_addendum(prob=0.7)]) == ["revisions_mismatch"]


@pytest.mark.parametrize(
    "revision",
    [
        {"at": "2026-10-05T06:26:24.047Z", "prob": 0.71},  # other value
        {"at": "2026-10-05T06:26:25.047Z", "prob": 0.7},  # other time
    ],
)
def test_an_addendum_probability_that_differs_from_its_revision_is_refused(
    revision: dict[str, Any],
) -> None:
    row = with_addendum(prob=0.7) | {"probRevisions": [revision]}
    assert check([row]) == ["revisions_mismatch"]


def test_the_match_compares_instants_not_spellings() -> None:
    revision = [{"at": "2026-10-05T14:26:24.047+08:00", "prob": 0.7}]
    assert check([with_addendum(prob=0.7) | {"probRevisions": revision}]) == []


def test_every_addendum_probability_needs_its_revision_and_extra_revisions_are_fine() -> None:
    first = {"at": "2026-10-05T06:26:24Z", "rationale": "x", "prob": 0.7}
    second = {"at": "2026-10-06T06:26:24Z", "rationale": "y", "prob": 0.6}
    revisions = [{"at": "2026-10-05T06:26:24Z", "prob": 0.7}]
    assert check([J(addenda=[first, second], probRevisions=revisions)]) == ["revisions_mismatch"]
    full = revisions + [{"at": "2026-10-06T06:26:24Z", "prob": 0.6}]
    assert check([J(addenda=[first, second], probRevisions=full)]) == []
    spoken = full + [{"at": "2026-10-07T06:26:24Z", "prob": 0.5, "note": "owner said so"}]
    assert check([J(addenda=[first, second], probRevisions=spoken)]) == []
    assert (
        check(
            [J(addenda=[{"at": "2026-10-05T06:26:24Z", "rationale": "z"}], probRevisions=revisions)]
        )
        == []
    )


def test_a_mismatch_is_reported_once_per_judgment() -> None:
    a = {"at": "2026-10-05T06:26:24Z", "rationale": "x", "prob": 0.7}
    b = {"at": "2026-10-06T06:26:24Z", "rationale": "y", "prob": 0.6}
    issues = schema.check_export(
        {"format": "judgments_export.v1", "exported_at": "2026-10-16T02:45:14Z",
         "judgments": [J(addenda=[a, b])]},
        known_tickers=KNOWN, method_ids=METHODS,
    )  # fmt: skip
    assert [(i.code, i.id, i.field) for i in issues] == [("revisions_mismatch", "a1", "addenda")]


@pytest.mark.parametrize("value", [1, 0, "0.7", 1.5, 0.0, 1.0, -0.2, True, None])
def test_an_addendum_probability_must_be_a_real_probability(value: Any) -> None:
    row = with_addendum(prob=value)
    assert check([row]) == ["schema"]


def test_an_addendum_probability_needs_a_readable_time() -> None:
    row = J(addenda=[{"at": "yesterday", "rationale": "x", "prob": 0.7}])
    assert check([row]) == ["schema"]
    row = J(addenda=[{"rationale": "x", "prob": 0.7}])
    assert check([row]) == ["schema"]


def test_addenda_without_probabilities_are_not_checked_for_revisions() -> None:
    assert check([J(addenda=[{"at": "2026-10-05T06:26:24Z", "rationale": "x"}])]) == []
    assert check([J(probRevisions=REV)]) == []


def test_old_addenda_still_have_to_stay_byte_equal_in_content() -> None:
    old = J(addenda=[{"at": "2026-10-05T06:26:24.047Z", "rationale": "x"}], probRevisions=REV)
    baseline = {"a1": copy.deepcopy(old)}
    stripped = copy.deepcopy(old)
    assert check([stripped], baseline) == []  # the exporter keeps old addenda as imported
    gained = copy.deepcopy(old)
    gained["addenda"][0]["prob"] = 0.7
    assert check([gained], baseline) == ["addenda_rewritten"]
    appended = copy.deepcopy(old)
    appended["addenda"].append({"at": "2026-10-06T06:26:24Z", "rationale": "y", "prob": 0.6})
    appended["probRevisions"].append({"at": "2026-10-06T06:26:24Z", "prob": 0.6})
    assert check([appended], baseline) == []


# ---------- the stored report ----------


def write_prices(path: Path) -> Path:
    days = pd.bdate_range("2026-01-05", periods=10)
    rows = [{"date": d.date().isoformat(), "ticker": t, "adj_close": 100.0}
            for d in days for t in ("TSM", "SMH")]  # fmt: skip
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def store_rows() -> list[dict[str, Any]]:
    return [
        R("a1", prob=0.65, revisions=(0.70,), status="hit"),
        R("a2", prob=0.65, revisions=(0.55, 0.35), methods=("capex_cycle", "guidance_base_rate"),
          status="miss"),
        R("a3", prob=0.70, status="miss"),
        R("lr", prob=0.75, purpose="learning", methods=("accounting_detail",), status="hit"),
        R("pn", deadline="2026-12-20", status="open", methods=("other",)),
    ]  # fmt: skip


def do_import(tmp_path: Path, rows: list[dict[str, Any]], exported_at: str) -> Path:
    data = tmp_path / "data" / "external" / "judgments"
    src = tmp_path / f"in_{exported_at[:19].replace(':', '')}.json"
    export = {"format": "judgments_export.v1", "exported_at": exported_at, "judgments": rows}
    src.write_text(json.dumps(export, ensure_ascii=False), encoding="utf-8")
    result = importer.import_export(
        src, data_dir=data, known_tickers=KNOWN, method_ids=METHODS, now=NOW, dry_run=False
    )
    assert result.status == "imported", result.issues
    return data


def settle_now(data: Path, tmp_path: Path, asof: date = ASOF) -> Any:
    params = SettleParams(
        market_timezone="America/New_York", market_close="16:00",
        max_calendar_gap_days=7, max_abs_daily_move=0.5,
    )  # fmt: skip
    result = run_settle(
        data_dir=data, prices_path=write_prices(tmp_path / "prices.csv"), params=params,
        asof=asof, now=NOW,
    )  # fmt: skip
    assert result.status in ("settled", "unchanged"), result.issues
    return result


def make(
    data: Path, asof: date = ASOF, dry_run: bool = False, params: ReportParams = PARAMS
) -> Any:
    return make_report(data_dir=data, params=params, asof=asof, dry_run=dry_run)


def snapshot(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()
    }


@pytest.fixture
def world(tmp_path: Path) -> Path:
    data = do_import(tmp_path, store_rows(), "2026-10-16T02:00:00Z")
    settle_now(data, tmp_path)
    return data


def test_first_report_writes_a_json_and_a_markdown_file(world: Path) -> None:
    result = make(world)
    assert result.status == "generated" and result.issues == []
    assert result.summary == {"methods": 2, "events": 1, "clusters": 1}
    assert result.name is not None and result.name.startswith("judgment_report_2026-10-16_")
    path = world / "reports" / result.name
    import hashlib

    raw = path.read_bytes()
    assert result.name == f"judgment_report_2026-10-16_{hashlib.sha256(raw).hexdigest()[:8]}.json"
    assert sorted(p.name for p in (world / "reports").iterdir()) == [
        result.name,
        result.name.replace(".json", ".md"),
    ]
    doc = json.loads(raw.decode("utf-8"))
    imports = json.loads((world / "imports.jsonl").read_text("utf-8").splitlines()[-1])
    settlements = json.loads((world / "settlements.jsonl").read_text("utf-8").splitlines()[-1])
    assert doc["export_sha256"] == imports["export_sha256"]
    assert doc["settlement_sha256"] == settlements["file_sha256"]
    assert doc["format"] == "judgment_report.v1" and doc["asof"] == "2026-10-16"
    g = method(doc, "guidance_base_rate")
    assert g["brier_scored"] == pytest.approx(0.7025 / 3, abs=1e-9) and g["n_events"] == 1
    assert doc["counts"] == {"settled": 3, "pending": 1, "void": 0, "data_gap": 0}
    assert doc["learning"]["n_judgments"] == 1
    assert doc["all_settled"]["brier_original"] == pytest.approx(0.274375, abs=1e-9)
    assert raw.endswith(b"\n")


def test_the_markdown_report_is_for_reading_and_holds_the_numbers(world: Path) -> None:
    result = make(world)
    assert result.name is not None
    text = (world / "reports" / result.name.replace(".json", ".md")).read_text("utf-8")
    assert text.startswith("# ")
    for needle in (
        "2026-10-16",
        "guidance_base_rate",
        "capex_cycle",
        "0.2342",
        "0.3450",
        "样本不足",
    ):
        assert needle in text, needle
    lines = [x for x in text.splitlines() if x.startswith("| guidance_base_rate ")]
    assert len(lines) >= 1


def test_report_files_hold_no_judgment_text(world: Path) -> None:
    make(world)
    for path in (world / "reports").iterdir():
        text = path.read_text("utf-8")
        assert SECRET not in text and NOTE_SECRET not in text, path.name


def test_same_inputs_again_is_unchanged_and_writes_nothing(world: Path) -> None:
    first = make(world)
    before = snapshot(world)
    again = make(world)
    assert again.status == "unchanged" and again.name == first.name and again.issues == []
    assert again.summary == first.summary
    assert snapshot(world) == before


def test_a_new_asof_or_new_settlement_makes_a_new_report(world: Path, tmp_path: Path) -> None:
    first = make(world)
    later = make(world, asof=date(2026, 10, 20))
    assert later.status == "generated" and later.name != first.name
    assert len(list((world / "reports").glob("*.json"))) == 2


def test_dry_run_writes_nothing_not_even_the_directory(world: Path) -> None:
    before = snapshot(world)
    result = make(world, dry_run=True)
    assert result.status == "generated" and result.name is not None
    assert snapshot(world) == before and not (world / "reports").exists()
    real = make(world)
    assert real.name == result.name  # the dry run named exactly what the real run writes
    assert make(world, dry_run=True).status == "unchanged"


def test_two_stores_with_the_same_inputs_give_byte_equal_reports(tmp_path: Path) -> None:
    names = []
    blobs = []
    for sub in ("one", "two"):
        root = tmp_path / sub
        root.mkdir()
        data = do_import(root, store_rows(), "2026-10-16T02:00:00Z")
        settle_now(data, root)
        result = make(data)
        names.append(result.name)
        assert result.name is not None
        blobs.append((data / "reports" / result.name).read_bytes())
    assert names[0] == names[1] and blobs[0] == blobs[1]


def codes(result: Any) -> list[str]:
    return [i.code for i in result.issues]


def test_no_import_and_no_settlement_are_refused(tmp_path: Path) -> None:
    result = make(tmp_path / "empty")
    assert result.status == "rejected" and codes(result) == ["no_import"]
    assert not (tmp_path / "empty").exists()
    data = do_import(tmp_path, store_rows(), "2026-10-16T02:00:00Z")
    before = snapshot(data)
    result = make(data)
    assert result.status == "rejected" and codes(result) == ["no_settlement"]
    assert snapshot(data) == before


def test_a_settlement_of_an_older_import_is_stale(world: Path, tmp_path: Path) -> None:
    rows = store_rows()
    rows[2]["status"] = "hit"  # a result changed in the ledger
    src = tmp_path / "second.json"
    export = {
        "format": "judgments_export.v1",
        "exported_at": "2026-10-17T02:00:00Z",
        "judgments": rows,
    }
    src.write_text(json.dumps(export, ensure_ascii=False), encoding="utf-8")
    done = importer.import_export(
        src, data_dir=world, known_tickers=KNOWN, method_ids=METHODS, now=NOW, dry_run=False
    )
    assert done.status == "imported"
    before = snapshot(world)
    result = make(world, asof=date(2026, 10, 18))
    assert result.status == "rejected" and codes(result) == ["settlement_stale"]
    assert snapshot(world) == before
    settle_now(world, tmp_path, asof=date(2026, 10, 18))
    assert make(world, asof=date(2026, 10, 18)).status == "generated"


def test_asof_cannot_precede_the_settlement(world: Path) -> None:
    result = make(world, asof=date(2026, 10, 15))
    assert result.status == "rejected" and codes(result) == ["asof_backwards"]
    assert not (world / "reports").exists()
    assert make(world, asof=ASOF).status == "generated"  # equal is fine


def test_a_tampered_import_copy_is_refused(world: Path) -> None:
    victim = next((world / "exports").iterdir())
    victim.write_bytes(victim.read_bytes() + b" ")
    result = make(world)
    assert result.status == "rejected" and codes(result) == ["baseline_tampered"]
    assert not (world / "reports").exists()


def test_a_tampered_settlement_file_or_chain_is_refused(world: Path) -> None:
    victim = next((world / "settlements").iterdir())
    victim.write_bytes(victim.read_bytes() + b" ")
    result = make(world)
    assert result.status == "rejected" and codes(result) == ["chain_broken"]
    assert not (world / "reports").exists()


def test_a_broken_import_chain_is_refused(world: Path) -> None:
    log = world / "imports.jsonl"
    log.write_bytes(log.read_bytes().replace(b'"seq":1', b'"seq":2'))
    result = make(world)
    assert result.status == "rejected" and codes(result) == ["chain_broken"]


def test_report_parameters_change_the_report(world: Path) -> None:
    default = make(world)
    other = make(world, params=PARAMS.model_copy(update={"confidence": 0.95}))
    assert other.name != default.name  # the Wilson interval moved


# ---------- command line ----------


def cli(args: list[str]) -> Any:
    return CliRunner().invoke(app, args)


def test_cli_report_prints_counts_only(tmp_path: Path) -> None:
    data = do_import(tmp_path, store_rows(), "2026-10-16T02:00:00Z")
    settle_now(data, tmp_path)
    out = cli(["report", "--root", str(tmp_path), "--asof", "2026-10-16"])
    assert out.exit_code == 0, out.output
    line = out.output.strip()
    assert line.startswith("reported file=judgment_report_2026-10-16_") and line.endswith(
        " asof=2026-10-16 methods=2 events=1 clusters=1"
    )
    assert SECRET not in out.output and NOTE_SECRET not in out.output
    name = line.split()[1].removeprefix("file=")
    assert (data / "reports" / name).is_file()
    again = cli(["report", "--root", str(tmp_path), "--asof", "2026-10-16"])
    assert again.exit_code == 0 and again.output.strip() == f"unchanged file={name}"


def test_cli_dry_run_line_and_nothing_written(tmp_path: Path) -> None:
    data = do_import(tmp_path, store_rows(), "2026-10-16T02:00:00Z")
    settle_now(data, tmp_path)
    out = cli(["report", "--root", str(tmp_path), "--asof", "2026-10-16", "--dry-run"])
    assert out.exit_code == 0, out.output
    assert out.output.strip() == "ok asof=2026-10-16 methods=2 events=1 clusters=1"
    assert not (data / "reports").exists()


def test_cli_rejection_prints_issue_codes_and_exits_one(tmp_path: Path) -> None:
    out = cli(["report", "--root", str(tmp_path / "none"), "--asof", "2026-10-16"])
    assert out.exit_code == 1
    assert out.output.splitlines() == [
        "issue no_import id=- field=-",
        "rejected: nothing was written",
    ]


def test_cli_asof_defaults_to_today_in_utc(tmp_path: Path) -> None:
    data = do_import(tmp_path, store_rows(), "2026-10-16T02:00:00Z")
    settle_now(data, tmp_path)
    before = datetime.now(UTC).date().isoformat()
    out = cli(["report", "--root", str(tmp_path), "--dry-run"])
    after = datetime.now(UTC).date().isoformat()
    assert out.exit_code == 0, out.output
    assert out.output.split()[1] in {f"asof={before}", f"asof={after}"}


def test_cli_rejects_a_bad_asof(tmp_path: Path) -> None:
    assert cli(["report", "--root", str(tmp_path), "--asof", "10/16/2026"]).exit_code != 0


def test_the_module_can_be_run_with_python_dash_m_and_offers_every_command() -> None:
    import os
    import subprocess
    import sys

    env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT / "src")}
    run = subprocess.run(
        [sys.executable, "-m", "ai_trading_system.research_judgments", "--help"],
        capture_output=True, text=True, env=env, cwd=PROJECT_ROOT, timeout=120,
    )  # fmt: skip
    assert run.returncode == 0, run.stderr
    for command in ("check", "import", "settle", "report"):
        assert command in run.stdout


# ---------- placement and documentation ----------


def test_files_are_in_the_right_zones() -> None:
    zones = yaml.safe_load((PROJECT_ROOT / "config" / "gov008_ship.yaml").read_text("utf-8"))[
        "zones"
    ]
    from fnmatch import fnmatch

    def zone_c(path: str) -> bool:
        return any(fnmatch(path, pattern) for pattern in zones["C"])

    assert zone_c("config/research/research_judgment_eval.yaml")
    for name in ("report", "reports"):
        assert not zone_c(f"src/ai_trading_system/research_judgments/{name}.py")
    assert not zone_c("tests/test_research_judgments_report.py")


def test_system_flow_mentions_the_report() -> None:
    flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text("utf-8")
    lines = [x for x in flow.splitlines() if "research_judgments" in x and "report" in x]
    assert len(lines) == 1
    for needle in ("judgment_report", "research_judgment_eval.yaml", "check|import|settle|report"):
        assert needle in lines[0], needle
    assert sum("TRADING-2569" in x for x in flow.splitlines()) == 1


def test_public_names() -> None:
    assert {"build_report", "classify", "load_report_params", "wilson", "cluster_stats"} <= set(
        dir(report)
    )
    assert {"make_report", "render_markdown"} <= set(dir(reports))


# ---------- second round: cases found by mutation testing ----------


def test_event_lift_uses_event_means_of_the_base_rate_error_and_the_scored_probability() -> None:
    rows = [
        R("a", prob=0.5, revisions=(0.9,), event="E1", deadline="2026-01-15", methods=("m",)),
        R("b", prob=0.7, event="E1", deadline="2026-01-15", methods=("m",), status="miss"),
        R("c", prob=0.8, event="E2", deadline="2026-04-15", methods=("m",)),
        R("d", prob=0.6, event="E3", deadline="2026-07-15", methods=("m",), status="miss"),
    ]
    g = method(rep(rows, [OC("a", 1), OC("b", 0), OC("c", 1), OC("d", 0)], LOOSE), "m")
    # event hit rates 0.5, 1, 0; leave-one-out base rates 0.5, 0.25, 0.75
    # base error: E1 (0.25 + 0.25) / 2, E2 0.5625, E3 0.5625
    # scored error: E1 (0.01 + 0.49) / 2 -- the original 0.5 would give 0.37 -- E2 0.04, E3 0.36
    expected = ((0.25 - 0.25) + (0.5625 - 0.04) + (0.5625 - 0.36)) / 3
    assert g["lift_mean"] == pytest.approx(expected, **APPROX)
    assert g["update_gain"] == pytest.approx((0.37 - 0.25) / 3, **APPROX)


def test_the_two_halves_are_ordered_by_event_date_before_event_id() -> None:
    spec = [(0.9, 0), (0.1, 1), (0.9, 0), (0.1, 1)] + [(0.9, 1), (0.1, 0)] * 4
    rows, outs = series(spec, "m")
    for i, row in enumerate(rows):  # ids run backwards while the dates run forwards
        row["eventId"] = f"e{99 - i:02d}"
    g = method(rep(rows, outs, LOOSE), "m")
    assert g["lift_first_half"] < 0 < g["lift_second_half"]


def test_groups_below_the_event_minimum_have_no_p_value_in_the_false_discovery_filter() -> None:
    spec = [(0.6, 0), (0.4, 0), (0.1, 0), (0.3, 0), (0.1, 0), (0.6, 1), (0.3, 0), (0.8, 1),
            (0.7, 1), (0.4, 0), (0.9, 1)]  # fmt: skip
    rows, outs = series(spec, "m")
    others = tuple(f"b{i:02d}" for i in range(10))  # ten thin ideas, three events each
    for i, (p, y) in enumerate([(0.95, 1), (0.05, 0), (0.95, 1)]):
        rows.append(R(f"t{i}", deadline=f"2025-0{3 * i + 1}-15", prob=p, methods=others,
                      status="hit" if y else "miss"))  # fmt: skip
        outs.append(OC(f"t{i}", y))
    params = LOOSE.model_copy(update={"bh_q": 0.03})
    doc = rep(rows, outs, params)
    assert doc["bh"]["m"] == 11
    thin = method(doc, "b00")
    assert thin["n_events"] < params.min_events and thin["lift_p"] == pytest.approx(
        0.0232954678, abs=1e-8
    )
    tested = method(doc, "m")
    assert tested["lift_p"] == pytest.approx(0.0040614597, abs=1e-8)
    # 0.0041 > 0.03 / 11: with the ten thin groups at p = 1 it fails; with their p-values it passes
    assert (tested["status"], tested["reason"]) == ("insufficient", "keep_collecting")


def test_calibration_leaves_out_the_practice_entries() -> None:
    rows, outs = real_shaped()
    doc = rep(rows, outs)
    assert [row["n"] for row in doc["calibration"]] == [
        0,
        1,
        2,
        0,
        0,
    ]  # the 0.75 practice entry is not in


def test_the_label_for_each_saw_system_group_matches_the_ledger() -> None:
    rows = [
        R("p", saw=True, deadline="2026-01-15", methods=("m",)),
        R("q", saw=False, deadline="2026-04-15", methods=("m",)),
        R("r", saw=False, deadline="2026-05-15", methods=("m",)),
        R("s", deadline="2026-07-15", methods=("m",)),
        R("t", deadline="2026-08-15", methods=("m",)),
        R("u", deadline="2026-09-15", methods=("m",)),
    ]
    doc = rep(rows, [OC(k, 1) for k in "pqrstu"])
    counts = {g["sawSystem"]: g["n_judgments"] for g in doc["by_saw_system"]}
    assert counts == {"yes": 1, "no": 2, "unknown": 3}


def test_a_missing_markdown_file_is_written_again_and_not_called_unchanged(world: Path) -> None:
    first = make(world)
    assert first.name is not None
    md = world / "reports" / first.name.replace(".json", ".md")
    md.unlink()
    again = make(world)
    assert again.status == "generated" and again.name == first.name
    assert md.is_file()


def test_the_summary_counts_clusters_not_events(tmp_path: Path) -> None:
    rows = [
        R("x1", deadline="2026-10-14", status="hit"),
        R("x2", deadline="2026-10-15", ticker="NVDA", status="miss"),
    ]
    data = do_import(tmp_path, rows, "2026-10-16T02:00:00Z")
    settle_now(data, tmp_path)
    result = make(data)
    assert result.summary == {"methods": 1, "events": 2, "clusters": 1}


def test_the_json_file_is_the_canonical_dump_of_its_content(world: Path) -> None:
    result = make(world)
    assert result.name is not None
    raw = (world / "reports" / result.name).read_bytes()
    canonical = json.dumps(json.loads(raw), sort_keys=True, ensure_ascii=False, indent=1) + "\n"
    assert raw == canonical.encode("utf-8")
