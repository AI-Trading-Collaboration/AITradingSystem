from __future__ import annotations

import json
import math
import re
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import (
    BenchData,
    from_daily_frame,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_bench.metrics import summarize
from ai_trading_system.research_bench.simulate import Rebalance, simulate
from ai_trading_system.research_probes import backfill_check as bc
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt

pytestmark = pytest.mark.filterwarnings("ignore:invalid value encountered in log1p")

FULL = load_settings()
# Fewer placebo and bootstrap draws than the bench uses: the same code paths, faster.
LOADED = replace(
    FULL,
    settings=FULL.settings.model_copy(
        update={
            "falsification": FULL.settings.falsification.model_copy(
                update={"placebo_draws": 10, "bootstrap_draws": 200}
            )
        }
    ),
)
ROOT_DIR = lg.research_root(LOADED)
PREREG = PROJECT_ROOT / "research" / "_trials" / "prereg.yaml"
GATE = tr.load_gate(PREREG)
_, PLAN = tr.load_trial(ROOT_DIR, pt.DATA_TRIAL)
SIGNAL_SNAPSHOT = PLAN.data.signal_snapshot_id
QQQ_SNAPSHOT = PLAN.data.snapshot_id
ROWS = {row.id: row for row in pt.table_rows(ROOT_DIR)}
# A fixed row and two overlays (one monthly, the weekly one) keep the whole-run tests fast.
RUN_ROWS = (ROWS["fixed_0.8"], ROWS["fast_slow"], ROWS["fast_slow_weekly"])


def _us_data(start: str = "1926-07-01", end: str = "2012-12-31", seed: int = 5) -> BenchData:
    """Daily market returns with volatility that changes month by month, running past 2010."""

    index = pd.bdate_range(start, end, name="date")
    rng = np.random.default_rng(seed)
    codes, uniques = pd.factorize(index.to_period("M"))
    monthly_vol = np.exp(rng.normal(math.log(0.01), 0.5, len(uniques)))
    factors = pd.DataFrame(
        {
            "Mkt-RF": rng.normal(0.0003, 1.0, len(index)) * monthly_vol[codes],
            "SMB": 0.0,
            "HML": 0.0,
            "RF": 0.0001,
        },
        index=index,
    )
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id=SIGNAL_SNAPSHOT)
    return with_signal_sources(data, {}, {}, snapshot_id=SIGNAL_SNAPSHOT)


def _qqq_data(end: str = "2012-12-31", seed: int = 3) -> BenchData:
    index = pd.bdate_range("1994-01-03", end, name="date")
    rng = np.random.default_rng(seed)
    r = rng.normal(0.0004, 0.015, len(index))
    level = 1000.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "etf_reference_return": r,
            "cash_return": 0.0001,
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=index,
    )
    data = from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": QQQ_SNAPSHOT})
    return with_signal_sources(data, {}, {}, snapshot_id=SIGNAL_SNAPSHOT)


RAW = {"us_market": _us_data(), "qqq": _qqq_data()}
DATA = {market: gp.cut(data) for market, data in RAW.items()}
PROVENANCE = {"commit": "abc1234", "dirty": False}


def _century(verdict: str = "holds", reason: str | None = None) -> dict[str, Any]:
    """A TRADING-2565-41 result as its JSON is written: only the fields the table reads."""

    def summary(count: int, median: float, positive: int) -> dict[str, Any]:
        return {
            "slow_bear": {
                "count": count,
                "median_difference": median,
                "positive": positive,
                "positive_share": positive / count,
                "min_difference": -0.04,
            },
            "fast_drop": {"count": 3, "median_difference": 0.01},
            "long_bull": {"count": 2, "median_annual_difference": -0.01},
        }

    return {
        "run_id": "TRADING-2565-41",
        "snapshot_id": "shiller-test",
        "sections": {
            "main": {
                "slow_only_m": {"summary": summary(6, -0.012, 2)},
                "fast_slow_m": {"summary": summary(6, 0.034, 5)},
                "trend4_m": {"summary": summary(6, 0.021, 4)},
            },
            "month_end": {},
        },
        "fidelity": {"holds": True, "variants": {}},
        "reading": {"verdict": verdict, "reason": reason, "holding": 2},
    }


@pytest.fixture(scope="module")
def probe() -> dict[str, Any]:
    return pt.run_probe(
        DATA, LOADED, GATE, ROOT_DIR, rows=RUN_ROWS, century=_century(), provenance=PROVENANCE
    )


def _found(result: dict[str, Any], market: str, row_id: str) -> dict[str, Any]:
    return next(r for r in result["results"] if r["market"] == market and r["row"] == row_id)


# --- rows -------------------------------------------------------------------------------------


def test_the_table_has_the_seven_fixed_rows_in_order() -> None:
    assert list(ROWS) == [
        "fixed_0.6",
        "fixed_0.8",
        "fixed_1.0",
        "fast_slow",
        "trend4",
        "slow_only",
        "fast_slow_weekly",
    ]
    for exposure in (0.6, 0.8, 1.0):
        row = ROWS[f"fixed_{exposure:.1f}"]
        assert row.kind == "fixed"
        assert row.function == "fixed_exposure"
        assert row.params == {"exposure": exposure}
        assert row.rebalance == Rebalance(schedule="month_end", band=0.0)
    assert "rv21" not in ROWS and "rv21_weekly" not in ROWS


@pytest.mark.parametrize(
    ("row_id", "forward", "question", "schedule"),
    [
        ("fast_slow", "F0001", "QQQ-Q3", "month_end"),
        ("trend4", "F0002", "QQQ-Q2", "month_end"),
        ("slow_only", "F0003", "QQQ-Q3", "month_end"),
        ("fast_slow_weekly", "F0004", "QQQ-Q3", "week_end"),
    ],
)
def test_the_overlays_are_the_backfill_configurations_unchanged(
    row_id: str, forward: str, question: str, schedule: str
) -> None:
    config = next(config for config in bc.CONFIGS if config.id == row_id)
    entry = bc.source_entry(ROOT_DIR, config)
    row = ROWS[row_id]
    assert row.kind == "overlay"
    assert row.function == entry.function == "signal_exposure"
    assert row.params == dict(entry.params)
    assert row.rebalance == entry.rebalance.to_rebalance()
    assert row.rebalance.schedule == schedule
    assert row.label.startswith(forward) and question in row.label


def test_the_windows_are_the_two_development_windows() -> None:
    us = pt.market_window(LOADED, GATE, "us_market")
    qqq = pt.market_window(LOADED, GATE, "qqq")
    assert (us.start, us.end) == (date(1929, 1, 2), date(2010, 12, 31))
    assert (qqq.start, qqq.end) == (date(1999, 3, 10), date(2010, 12, 31))
    assert us.role == qqq.role == "development"


def test_the_run_spec_is_the_backfill_one() -> None:
    config = bc.CONFIGS[0]
    entry = bc.source_entry(ROOT_DIR, config)
    ours = pt.run_spec(ROWS["fast_slow"], "us_market", GATE)
    theirs = bc.run_spec(config, entry, GATE)
    assert ours == theirs
    qqq = pt.run_spec(ROWS["fast_slow"], "qqq", GATE)
    assert qqq.question_type == GATE.markets["qqq"].question_type
    assert (qqq.delay_days, qqq.leveraged_benchmarks, qqq.reserve) == (0, False, "cash")


# --- check ------------------------------------------------------------------------------------


def test_check_passes_on_data_that_cover_both_windows() -> None:
    rows = pt.check_targets(list(ROWS.values()), DATA, LOADED, GATE)
    assert len(rows) == 14
    assert [row["market"] for row in rows] == ["us_market"] * 7 + ["qqq"] * 7
    assert all(row["ok"] and row["missing_days"] == 0 for row in rows)
    assert {row["window_id"] for row in rows} == {
        "us_market_1929_2010",
        "qqq_long_history_development",
    }


def test_check_fails_a_signal_without_enough_history() -> None:
    short = {**DATA, "us_market": gp.cut(_us_data(start="1928-09-03"))}
    rows = {(r["market"], r["row"]): r for r in pt.check_targets(RUN_ROWS, short, LOADED, GATE)}
    assert rows[("us_market", "fixed_0.8")]["ok"]
    late = rows[("us_market", "fast_slow")]
    assert not late["ok"] and late["missing_days"] > 0
    assert rows[("qqq", "fast_slow")]["ok"]


def test_check_reports_an_error_without_stopping() -> None:
    broken = pt.Row("broken", "fixed", "broken", "fixed_exposure", {"exposure": "x"})
    rows = pt.check_targets([broken, ROWS["fixed_0.8"]], DATA, LOADED, GATE)
    assert [row["ok"] for row in rows] == [False, True, False, True]
    assert "error" in rows[0] and "ValueError" in rows[0]["error"]


# --- one row ----------------------------------------------------------------------------------


def test_a_fixed_row_reproduces_an_overlays_matched_fixed_exposure(probe: dict[str, Any]) -> None:
    """Requirement test (a): a fixed row at an overlay's average exposure is the overlay's main
    benchmark, number for number, in window A."""

    overlay = _found(probe, "us_market", "fast_slow")
    exposure = overlay["strategy"]["average_exposure"]
    row = pt.Row("fixed_at", "fixed", "fixed", "fixed_exposure", {"exposure": exposure})
    row = replace(row, rebalance=ROWS["fast_slow"].rebalance)
    fixed = pt.evaluate(row, "us_market", DATA["us_market"], LOADED, GATE)
    assert fixed["status"] == "ok"
    for key in pt.METRICS:
        assert fixed["strategy"][key] == pytest.approx(overlay["benchmark"][key], rel=1e-12)


def test_the_fixed_row_is_the_bench_simulation_of_a_constant(probe: dict[str, Any]) -> None:
    found = _found(probe, "us_market", "fixed_0.8")
    window = pt.market_window(LOADED, GATE, "us_market")
    data = DATA["us_market"]
    targets = pd.Series(0.8, index=data.returns.index)
    sim = simulate(
        data.returns,
        targets,
        start=window.start,
        end=window.end,
        rebalance=Rebalance("month_end", 0.0),
        cost_bps=LOADED.cost_bps,
    )
    expected = summarize(sim, data.returns["cash"], LOADED)
    for key in pt.METRICS:
        assert found["strategy"][key] == pytest.approx(expected[key], rel=1e-12)


@pytest.mark.parametrize("market", pt.MARKETS)
@pytest.mark.parametrize("row_id", [row.id for row in RUN_ROWS])
def test_the_25_bps_difference_is_the_falsification_cost_25(
    probe: dict[str, Any], market: str, row_id: str
) -> None:
    cost = _found(probe, market, row_id)["cost"]
    assert cost["bps"] == 25.0
    assert cost["difference"] == pytest.approx(cost["falsification"], abs=1e-12)
    assert cost["difference"] == pytest.approx(cost["strategy"] - cost["benchmark"], abs=1e-12)


def test_the_25_bps_geometric_return_is_resimulated_at_25_bps(probe: dict[str, Any]) -> None:
    found = _found(probe, "us_market", "fast_slow")
    window = pt.market_window(LOADED, GATE, "us_market")
    data = DATA["us_market"]
    row = ROWS["fast_slow"]
    targets = strategy_targets(tr.STRATEGY_FUNCTIONS[row.function], data.inputs, row.params)
    sim = simulate(
        data.returns,
        targets,
        start=window.start,
        end=window.end,
        rebalance=row.rebalance,
        cost_bps=25.0,
    )
    expected = summarize(sim, data.returns["cash"], LOADED)["geometric_annual_return"]
    assert found["cost"]["strategy"] == pytest.approx(expected, rel=1e-12)
    assert found["cost"]["strategy"] < found["strategy"]["geometric_annual_return"]


@pytest.mark.parametrize("market", pt.MARKETS)
def test_the_segments_are_the_gates_registered_lists(probe: dict[str, Any], market: str) -> None:
    window = pt.market_window(LOADED, GATE, market)
    slow_bears, long_bulls = GATE.markets[market].segment_lists(window.window_id)
    assert slow_bears and long_bulls
    for row in RUN_ROWS:
        segments = _found(probe, market, row.id)["segments"]
        assert [s["id"] for s in segments["slow_bear"]] == [s.id for s in slow_bears]
        assert [s["id"] for s in segments["long_bull"]] == [s.id for s in long_bulls]
        assert segments["slow_bear_summary"]["count"] == len(slow_bears)
        assert segments["long_bull_summary"]["count"] == len(long_bulls)


def test_an_overlay_row_carries_comparison_events_and_window(probe: dict[str, Any]) -> None:
    found = _found(probe, "us_market", "fast_slow")
    assert found["status"] == "ok" and found["kind"] == "overlay"
    assert set(found["strategy"]) == set(pt.METRICS)
    assert set(found["benchmark"]) == set(pt.METRICS)
    assert set(found["comparison"]) == set(pt.COMPARED)
    events = found["events"]
    assert set(events) == {"count", "protective", "false_alarm", "neutral"}
    assert events["count"] == events["protective"] + events["false_alarm"] + events["neutral"] > 0
    assert found["window"]["window_id"] == "us_market_1929_2010"
    assert found["comparison"]["max_drawdown"] == pytest.approx(
        found["strategy"]["max_drawdown"] - found["benchmark"]["max_drawdown"], abs=1e-12
    )


def test_a_failing_row_is_reported_not_raised() -> None:
    broken = pt.Row("broken", "fixed", "broken", "fixed_exposure", {"exposure": 5.0})
    found = pt.evaluate(broken, "qqq", DATA["qqq"], LOADED, GATE)
    assert found["status"] in {"error", "check_failed"}
    assert found["row"] == "broken" and found["market"] == "qqq"


def test_segment_tables_add_medians_and_positive_counts() -> None:
    section = {
        "slow_bear": [
            {"id": "a", "difference": 0.05, "strategy_return": -0.10},
            {"id": "b", "difference": -0.01, "strategy_return": -0.30},
            {"id": "c", "difference": 0.02, "strategy_return": -0.20},
            {"id": "d", "difference": None, "strategy_return": None},
            {"id": "e", "difference": 0.0, "strategy_return": -0.15},
        ],
        "long_bull": [
            {"id": "x", "annual_difference": -0.02, "strategy_annual": 0.10},
            {"id": "y", "annual_difference": -0.01, "strategy_annual": 0.14},
            {"id": "w", "annual_difference": -0.06, "strategy_annual": 0.11},
            {"id": "z", "annual_difference": None, "strategy_annual": None},
        ],
        "slow_bear_summary": {"count": 4, "median_difference": 0.01},
        "long_bull_summary": {"count": 3, "mean_annual_difference": -0.03},
    }
    out = pt.segment_tables(section)
    assert out["slow_bear_summary"]["positive"] == 2
    assert out["slow_bear_summary"]["median_strategy_return"] == pytest.approx(-0.175)
    assert out["slow_bear_summary"]["median_difference"] == 0.01
    assert out["slow_bear_summary"]["count"] == 4
    assert out["long_bull_summary"] == {
        "count": 3,
        "median_annual_difference": pytest.approx(-0.02),
        "median_strategy_annual": pytest.approx(0.11),
    }
    assert len(out["slow_bear"]) == 5 and len(out["long_bull"]) == 4
    empty = pt.segment_tables(None)
    assert empty["slow_bear_summary"]["positive"] == 0
    assert empty["long_bull_summary"]["median_annual_difference"] is None


# --- data after 2010 --------------------------------------------------------------------------


def _spoiled(data: BenchData) -> BenchData:
    late = pd.DatetimeIndex(data.returns.index) > pd.Timestamp("2010-12-31")
    returns = data.returns.copy()
    returns.loc[late, :] = np.nan
    inputs = data.inputs.copy()
    inputs.loc[pd.DatetimeIndex(inputs.index) > pd.Timestamp("2010-12-31"), :] = np.nan
    return BenchData(returns=returns, inputs=inputs, source=dict(data.source))


def _garbled(data: BenchData) -> BenchData:
    """Data after 2010 replaced by a crash, not cut: the table must not move."""

    late = pd.DatetimeIndex(data.returns.index) > pd.Timestamp("2010-12-31")
    returns = data.returns.copy()
    returns.loc[late, "qqq"] = -0.05
    inputs = data.inputs.copy()
    inputs_late = pd.DatetimeIndex(inputs.index) > pd.Timestamp("2010-12-31")
    numeric = inputs.select_dtypes("number").columns
    inputs.loc[inputs_late, numeric] = inputs.loc[inputs_late, numeric] * 0.5
    return BenchData(returns=returns, inputs=inputs, source=dict(data.source))


def test_load_data_cuts_both_markets_at_2010(monkeypatch: pytest.MonkeyPatch) -> None:
    """Requirement test (c): nothing after 2010-12-31 reaches the table."""

    calls: list[str] = []

    def market_data(market: str, gate: Any, plan: Any, **kwargs: Any) -> BenchData:
        calls.append(market)
        return RAW[market]

    monkeypatch.setattr(pt, "load_settings", lambda: LOADED)
    monkeypatch.setattr(tr, "market_data", market_data)
    datasets, gate, loaded, root_dir = pt.load_data(Path("unused"))
    assert calls == ["us_market", "qqq"]
    assert root_dir == ROOT_DIR and loaded is LOADED
    for market, data in datasets.items():
        assert RAW[market].returns.index.max() > pd.Timestamp("2010-12-31")
        assert data.returns.index.max() <= pd.Timestamp("2010-12-31")
        assert data.inputs.index.max() <= pd.Timestamp("2010-12-31")


def test_load_data_refuses_data_of_another_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    other = BenchData(
        returns=RAW["qqq"].returns,
        inputs=RAW["qqq"].inputs,
        source={**RAW["qqq"].source, "snapshot_id": "qlh-other"},
    )
    monkeypatch.setattr(pt, "load_settings", lambda: LOADED)
    monkeypatch.setattr(
        tr, "market_data", lambda market, *a, **k: other if market == "qqq" else RAW[market]
    )
    with pytest.raises(ValueError, match="qqq: long-history snapshot"):
        pt.load_data(Path("unused"))


def test_spoiled_data_after_2010_change_nothing(probe: dict[str, Any]) -> None:
    for spoiled in (gp.cut(_spoiled(RAW["qqq"])), _garbled(RAW["qqq"])):
        again = pt.evaluate(ROWS["fast_slow"], "qqq", spoiled, LOADED, GATE)
        assert again == _found(probe, "qqq", "fast_slow")


# --- window C ---------------------------------------------------------------------------------


def _validation_table(text: str) -> tuple[str, list[list[str]]]:
    lines = text.splitlines()
    first = next(
        i for i, line in enumerate(lines) if line.startswith("和平均敞口相同的固定敞口相比")
    )
    table = []
    for line in lines[first + 1 :]:
        if table and not line.startswith("|"):
            break
        if line.startswith("|") and not line.startswith("|---"):
            table.append([cell.strip() for cell in line.strip().strip("|").split("|")])
    return lines[first], table


@pytest.mark.parametrize("overlay", list(pt.VALIDATION))
def test_every_validation_cell_is_in_its_published_decision_record(overlay: str) -> None:
    """Requirement test (c): window C is copied, cell by cell, from the published decision.md."""

    item = pt.VALIDATION[overlay]
    text = (PROJECT_ROOT / "research" / item.question / "decision.md").read_text(encoding="utf-8")
    assert f"`{item.run_id}`" in text
    caption, table = _validation_table(text)
    assert f"区间为 {item.interval}" in caption
    header, *rows = table
    assert f"{item.exposure}）" in header[item.column]
    cells = {row[0]: row[item.column] for row in rows}
    for field, label in pt.VALIDATION_ROWS.items():
        assert cells[label] == getattr(item, field)


def test_the_validation_column_carries_record_ids_and_skips_fixed_rows(
    probe: dict[str, Any],
) -> None:
    column = pt.validation_column()
    assert set(column) == {"fast_slow", "trend4", "slow_only", "fast_slow_weekly"}
    assert column["slow_only"]["run_id"] == "QQQ-Q3-r0009"
    assert column["trend4"]["source"] == "research/QQQ-Q2/decision.md"
    assert all(cell["run_id"] and cell["source"] for cell in column.values())
    summary = {cells["row"]: cells for cells in probe["summary"]}
    assert summary["fixed_0.8"]["validation"] is None
    assert summary["fast_slow"]["validation"] == column["fast_slow"]


def test_the_module_reads_no_research_record() -> None:
    source = Path(pt.__file__).read_text(encoding="utf-8")
    assert "runs" not in re.findall(r"\w+", source)
    assert "trial_records" not in source and "window_reads" not in source


# --- window D ---------------------------------------------------------------------------------


def test_the_century_column_is_pending_without_a_result() -> None:
    assert pt.century_column(None) == {"status": "pending"}


def test_the_century_column_takes_the_month_equivalents() -> None:
    column = pt.century_column(_century("unread", "fidelity"))
    assert column["status"] == "filled"
    assert column["verdict"] == "unread" and column["reason"] == "fidelity"
    assert column["fidelity_holds"] is True
    assert set(column["configs"]) == {"fast_slow", "trend4", "slow_only"}
    assert column["configs"]["fast_slow"]["config"] == "fast_slow_m"
    assert column["configs"]["fast_slow"]["median_difference"] == 0.034
    assert column["configs"]["slow_only"]["positive"] == 2
    assert column["configs"]["trend4"]["positive_share"] == pytest.approx(4 / 6)


def test_the_century_column_refuses_another_run() -> None:
    with pytest.raises(ValueError, match="TRADING-2565-41"):
        pt.century_column({**_century(), "run_id": "TRADING-2565-38"})


# --- backfill check ---------------------------------------------------------------------------


def _backfill_rows(**changes: float) -> list[dict[str, Any]]:
    rows = []
    for overlay, expected in pt.BACKFILL.items():
        row: dict[str, Any] = {"row": overlay, "market": "us_market", "status": "ok"}
        for key, value in expected.items():
            section, name = key.split(".")
            row.setdefault(section, {})[name] = changes.get(f"{overlay}.{key}", value)
        rows.append(row)
    rows.append({"row": "fast_slow", "market": "qqq", "status": "ok", "comparison": {}})
    return rows


def test_the_backfill_check_is_consistent_on_the_backfill_numbers() -> None:
    found = pt.backfill_check(_backfill_rows())
    assert [item["row"] for item in found] == list(pt.BACKFILL)
    assert all(item["consistent"] and item["differs"] == [] for item in found)
    near = pt.backfill_check(
        _backfill_rows(**{"trend4.comparison.max_drawdown": 0.2062102927772601 + 5e-10})
    )
    assert all(item["consistent"] for item in near)


def test_the_backfill_check_names_what_differs() -> None:
    rows = _backfill_rows(
        **{
            "trend4.comparison.max_drawdown": 0.2062102927772601 + 2e-9,
            "trend4.events.protective": 19,
        }
    )
    found = {item["row"]: item for item in pt.backfill_check(rows)}
    assert found["trend4"] == {
        "row": "trend4",
        "consistent": False,
        "differs": ["comparison.max_drawdown", "events.protective"],
    }
    assert found["fast_slow"]["consistent"]
    missing = pt.backfill_check([r for r in rows if r["row"] != "slow_only"])
    assert {item["row"]: item["differs"] for item in missing}["slow_only"] == ["没有算出"]


# The four overlays' full-precision records in the backfill report (TRADING-2567-33), copied here
# separately from the module's list.
BACKFILL_REPORT = {
    "fast_slow": (
        0.06762813626462504,
        0.19247427098472558,
        0.013132650009683555,
        0.006261356936889451,
        26,
        18,
        102,
    ),
    "trend4": (
        0.06032950764785847,
        0.2062102927772601,
        0.010395077809789433,
        0.005237110524457522,
        18,
        13,
        101,
    ),
    "slow_only": (
        -0.008393312442394343,
        0.24354415926075046,
        0.0170758127781534,
        0.015438007360233152,
        6,
        22,
        5,
    ),
    "fast_slow_weekly": (
        0.10421983318329192,
        0.33642788573929694,
        0.015348434071946526,
        0.003122447268804729,
        28,
        14,
        293,
    ),
}
BACKFILL_KEYS = (
    "comparison.cvar_5pct_relative_improvement",
    "comparison.max_drawdown",
    "comparison.geometric_annual_return",
    "cost.falsification",
    "events.protective",
    "events.false_alarm",
    "events.neutral",
)


def test_the_backfill_numbers_are_the_full_precision_report_ones() -> None:
    assert pt.BACKFILL_RUN_ID == "TRADING-2567-33" and pt.BACKFILL_TOLERANCE == 1e-9
    assert pt.BACKFILL == {
        overlay: dict(zip(BACKFILL_KEYS, values, strict=True))
        for overlay, values in BACKFILL_REPORT.items()
    }
    # Requirement 4(b)'s rounded numbers.
    assert round(pt.BACKFILL["fast_slow"]["comparison.cvar_5pct_relative_improvement"], 3) == 0.068
    assert round(pt.BACKFILL["trend4"]["comparison.max_drawdown"], 3) == 0.206
    assert (
        round(pt.BACKFILL["fast_slow_weekly"]["comparison.cvar_5pct_relative_improvement"], 3)
        == 0.104
    )


def test_window_a_is_the_backfill_probes_evaluation(probe: dict[str, Any]) -> None:
    """Requirement test (b) on synthetic data: an overlay's window A comparison, 25 bps difference
    and derisking events are the backfill probe's for the same configuration and data."""

    config = next(config for config in bc.CONFIGS if config.id == "fast_slow")
    theirs = bc.evaluate(config, DATA["us_market"], LOADED, GATE, ROOT_DIR)
    ours = _found(probe, "us_market", "fast_slow")
    assert theirs["status"] == ours["status"] == "ok"
    for key in pt.COMPARED:
        assert ours["comparison"][key] == theirs["comparison"][key]
    cost = next(r for r in theirs["gate"] if r["metric"] == "cost_25.geometric_annual_return")
    assert ours["cost"]["falsification"] == cost["observed"]
    assert ours["events"]["count"] == theirs["events"]["count"]
    for key in ("protective", "false_alarm", "neutral"):
        assert ours["events"][key] == theirs["events"]["outcomes"][key]
    synthetic = pt.backfill_check(probe["results"])
    assert next(item for item in synthetic if item["row"] == "fast_slow")["differs"] == list(
        BACKFILL_KEYS
    )


# --- summary ----------------------------------------------------------------------------------


def test_the_summary_cells_follow_the_row_kind(probe: dict[str, Any]) -> None:
    summary = {cells["row"]: cells for cells in probe["summary"]}
    assert list(summary) == [row.id for row in RUN_ROWS]
    fixed = summary["fixed_0.8"]["us_market"]
    found = _found(probe, "us_market", "fixed_0.8")
    assert fixed["max_drawdown"] == found["strategy"]["max_drawdown"]
    assert fixed["max_drawdown_difference"] is None and fixed["geometric_difference"] is None
    assert (
        fixed["slow_bear_median"]
        == found["segments"]["slow_bear_summary"]["median_strategy_return"]
    )
    assert fixed["slow_bear_positive"] is None
    assert (
        fixed["long_bull_median"]
        == found["segments"]["long_bull_summary"]["median_strategy_annual"]
    )
    overlay = summary["fast_slow"]["qqq"]
    found = _found(probe, "qqq", "fast_slow")
    assert overlay["geometric"] == found["strategy"]["geometric_annual_return"]
    assert overlay["geometric_difference"] == found["comparison"]["geometric_annual_return"]
    assert overlay["max_drawdown_difference"] == found["comparison"]["max_drawdown"]
    assert (
        overlay["slow_bear_median"] == found["segments"]["slow_bear_summary"]["median_difference"]
    )
    assert overlay["slow_bear_positive"] == found["segments"]["slow_bear_summary"]["positive"]
    assert (
        overlay["long_bull_median"]
        == found["segments"]["long_bull_summary"]["median_annual_difference"]
    )
    assert summary["fast_slow"]["century"]["config"] == "fast_slow_m"
    assert summary["fast_slow_weekly"]["century"] is None


@pytest.mark.parametrize(
    ("bull", "bear", "text"),
    [
        (-0.012, 0.05, "fast_slow：1929–2010 每年在长牛里少 1.2 点，慢熊里段内多 5.0 点（中位）"),
        (0.004, -0.021, "fast_slow：1929–2010 每年在长牛里多 0.4 点，慢熊里段内少 2.1 点（中位）"),
    ],
)
def test_the_tradeoff_line_is_mechanical(bull: float, bear: float, text: str) -> None:
    cells = {
        "row": "fast_slow",
        "us_market": {"long_bull_median": bull, "slow_bear_median": bear, "slow_bear_positive": 3},
    }
    assert pt.tradeoff_line(cells) == text


def test_there_is_no_tradeoff_line_for_a_fixed_row_or_a_missing_market() -> None:
    fixed = {
        "row": "fixed_0.8",
        "us_market": {
            "long_bull_median": 0.1,
            "slow_bear_median": -0.3,
            "slow_bear_positive": None,
        },
    }
    assert pt.tradeoff_line(fixed) is None
    assert pt.tradeoff_line({"row": "trend4", "us_market": None}) is None


# --- whole run --------------------------------------------------------------------------------


def test_the_run_records_its_sources(probe: dict[str, Any]) -> None:
    assert probe["run_id"] == "TRADING-2565-42"
    assert probe["owner_decision"] == "owner_decision:TRADING-2565:2026-10-21:period3_direction"
    assert probe["data_trial"] == "T0004"
    assert probe["gate_sha256"] == lg.chain_digest(PREREG.read_bytes())
    assert probe["sources"]["us_market"]["signal_snapshot_id"] == SIGNAL_SNAPSHOT
    assert probe["sources"]["qqq"]["snapshot_id"] == QQQ_SNAPSHOT
    assert probe["settings_sha256"] == LOADED.sha256
    assert probe["windows"] == {
        "us_market": {
            "window_id": "us_market_1929_2010",
            "start": "1929-01-02",
            "end": "2010-12-31",
        },
        "qqq": {
            "window_id": "qqq_long_history_development",
            "start": "1999-03-10",
            "end": "2010-12-31",
        },
    }
    assert probe["provenance"] == PROVENANCE
    assert probe["rows"] == [{"id": r.id, "kind": r.kind, "label": r.label} for r in RUN_ROWS]
    assert [(r["market"], r["row"]) for r in probe["results"]] == [
        (market, row.id) for market in pt.MARKETS for row in RUN_ROWS
    ]
    assert probe["complete"] is True
    assert probe["century"]["status"] == "filled"
    assert [item["row"] for item in probe["backfill"]] == list(pt.BACKFILL)
    assert (probe["production_effect"], probe["broker_action"]) == ("none", "none")


def test_the_run_reports_progress_and_an_incomplete_table() -> None:
    seen: list[tuple[str, str, str]] = []
    broken = pt.Row("broken", "fixed", "坏的一行", "fixed_exposure", {"exposure": 5.0})
    result = pt.run_probe(
        DATA,
        LOADED,
        GATE,
        ROOT_DIR,
        rows=[broken],
        provenance=PROVENANCE,
        progress=lambda market, row_id, found: seen.append((market, row_id, found["status"])),
    )
    assert [(m, r) for m, r, _ in seen] == [("us_market", "broken"), ("qqq", "broken")]
    assert result["complete"] is False
    assert result["century"] == {"status": "pending"}
    assert all(not item["consistent"] for item in result["backfill"])
    text = pt.render_markdown(result)
    assert "| 坏的一行 | 算不出：" in text
    assert "| 坏的一行 | 算不出 | 算不出 | 无记录 | 待乙 |" in text


def test_the_result_is_plain_json(probe: dict[str, Any], tmp_path: Path) -> None:
    pt.write_result(probe, tmp_path)
    found = json.loads((tmp_path / pt.RESULT_JSON).read_text(encoding="utf-8"))
    assert found["run_id"] == probe["run_id"]
    assert (tmp_path / pt.RESULT_MD).read_text(encoding="utf-8") == pt.render_markdown(probe)
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted([pt.RESULT_JSON, pt.RESULT_MD])


# --- report -----------------------------------------------------------------------------------


def _summary_table(text: str) -> dict[str, list[str]]:
    section = text.split("## 汇总")[1].split("\n## ")[0]
    return {cells[0]: cells for cells in _tables(section) if cells[0] != "方案"}


def _tables(text: str) -> list[list[str]]:
    return [
        [cell.strip() for cell in line.strip().strip("|").split("|")]
        for line in text.splitlines()
        if line.startswith("|") and not line.startswith("|---")
    ]


def test_the_report_has_no_data_rows(probe: dict[str, Any]) -> None:
    """Requirement test (d): every table line is a header or one row of the table."""

    text = pt.render_markdown(probe)
    labels = {row.label for row in RUN_ROWS}
    headers = {"方案"}
    for cells in _tables(text):
        assert cells[0].split("（和回填检验不一致：")[0] in labels | headers, cells
    assert text.count("（和回填检验不一致：") <= len(pt.BACKFILL)
    assert not re.search(r"\d{4}-\d{2}-\d{2} \|", text)
    segments = sum(
        len(GATE.markets[m].segment_lists(pt.market_window(LOADED, GATE, m).window_id)[i])
        for m in pt.MARKETS
        for i in (0, 1)
    )
    assert len(_tables(text)) <= len(RUN_ROWS) * (segments + 12) + 12


def test_the_report_header_and_sections(probe: dict[str, Any]) -> None:
    text = pt.render_markdown(probe)
    assert text.startswith("# 日报方案决定表：固定敞口和趋势覆盖\n")
    assert "TRADING-2565-42" in text and pt.OWNER_DECISION in text
    assert "提交 abc1234" in text and SIGNAL_SNAPSHOT in text and QQQ_SNAPSHOT in text
    assert probe["gate_sha256"] in text and "计划 T0004" in text
    assert "载入后截到 2010-12-31" in text
    for heading in (
        "## 汇总",
        "## 美股大盘 1929–2010（开发窗口 us_market_1929_2010，1929-01-02..2010-12-31）",
        "## QQQ 1999–2010（开发窗口 qqq_long_history_development，1999-03-10..2010-12-31）",
        "### 和回填检验（TRADING-2567-33）核对，只印",
        "## 2011–2021 验证窗口（只抄已发布的 decision.md，相对同敞口固定敞口）",
        "## 1871–1928（乙的探针，月频等价，不是同一配置）",
    ):
        assert heading in text
    assert text.rstrip().endswith("production_effect=none；broker_action=none。")


def test_the_report_copies_window_c_with_intervals(probe: dict[str, Any]) -> None:
    text = pt.render_markdown(probe)
    label = ROWS["fast_slow"].label
    assert (
        f"| {label} | `QQQ-Q3-r0008`（research/QQQ-Q3/decision.md） | 95% 平稳 bootstrap | 0.82 | "
        "+5.3 个点（−6.1..+8.4） | +4.4%（−6.5..+14.3） | −1.8 个点（−5.4..+1.5） | −2.5 个点 |"
    ) in text
    assert f"| {ROWS['fixed_0.8'].label} | 无记录 | — | — | — | — | — | — |" in text
    summary = _summary_table(text)[label]
    assert summary[3] == "+4.4%（−6.5..+14.3） / −1.8 个点（−5.4..+1.5）（`QQQ-Q3-r0008`）"
    assert summary[4] == "+3.4 / 83.3%"


def test_the_report_fills_window_d(probe: dict[str, Any]) -> None:
    text = pt.render_markdown(probe)
    assert (
        "- 来源 TRADING-2565-41，快照 shiller-test；"
        "乙的读法：趋势覆盖的慢熊效果在 1871–1928 样本外成立。" in text
    )
    assert f"| {ROWS['fast_slow'].label} | fast_slow_m | 6 | +3.4 | 5 | 83.3% |" in text
    assert f"| {ROWS['fast_slow_weekly'].label} | 没有月频等价 | — | — | — | — |" in text
    assert f"| {ROWS['fixed_0.8'].label} | 无 | — | — | — | — |" in text
    rows = _summary_table(text)
    assert rows[ROWS["fast_slow_weekly"].label][4] == "没有月频等价"
    assert rows[ROWS["fixed_0.8"].label][4] == "无"
    unread = {**probe, "century": pt.century_column(_century("unread", "few_slow_bears"))}
    assert "乙的读法：只印不读（慢熊段少于 4 段）。" in pt.render_markdown(unread)


def test_the_report_without_window_d_says_pending(probe: dict[str, Any]) -> None:
    century = pt.century_column(None)
    pending = {
        **probe,
        "century": century,
        "summary": pt.summary_rows(probe["results"], probe["validation"], century),
    }
    text = pt.render_markdown(pending)
    assert "待乙：跑这张表时没有给乙的结果文件。" in text
    rows = _summary_table(text)
    assert len(rows) == len(RUN_ROWS) and all(c[4] == "待乙" for c in rows.values())


def test_the_report_prints_absolute_numbers_with_differences(probe: dict[str, Any]) -> None:
    text = pt.render_markdown(probe)
    found = _found(probe, "qqq", "fast_slow")
    geometric = f"{found['strategy']['geometric_annual_return'] * 100:.1f}%"
    difference = f"{found['comparison']['geometric_annual_return'] * 100:+.1f}"
    events = found["events"]
    cvar = f"{found['comparison']['cvar_5pct_relative_improvement'] * 100:+.1f}%"
    line = next(
        line
        for line in text.splitlines()
        if line.startswith(f"| {ROWS['fast_slow'].label} | {geometric}（{difference}）")
    )
    assert line.endswith(
        f"| {events['protective']} / {events['false_alarm']} / {events['neutral']} |"
    )
    assert f"%（{cvar}） | " in line
    fixed = _found(probe, "us_market", "fixed_0.8")
    slow = fixed["segments"]["slow_bear_summary"]
    bull = fixed["segments"]["long_bull_summary"]
    label = ROWS["fixed_0.8"].label
    median = f"{slow['median_strategy_return'] * 100:+.1f}"
    assert f"| {label} | {slow['count']} | {median} | — | — |" in text
    assert f"| {label} | {bull['count']} | {bull['median_strategy_annual'] * 100:+.1f} |" in text
    geometric = fixed["strategy"]["geometric_annual_return"]
    fixed_line = f"| {ROWS['fixed_0.8'].label} | {geometric * 100:.1f}% | "
    assert any(line.startswith(fixed_line) and line.endswith("| — |") for line in text.splitlines())


def test_the_report_states_the_backfill_check(probe: dict[str, Any]) -> None:
    consistent = {
        **probe,
        "backfill": [{"row": r, "consistent": True, "differs": []} for r in pt.BACKFILL],
    }
    text = pt.render_markdown(consistent)
    assert "- 四个覆盖在 1929–2010 和回填检验（TRADING-2567-33）一致。" in text
    assert f"- {ROWS['fast_slow'].label}：一致" in text
    assert "不一致" not in text


def test_an_inconsistent_backfill_check_is_marked_in_the_header_and_window_a(
    probe: dict[str, Any],
) -> None:
    marked = {
        **probe,
        "backfill": [
            {"row": "fast_slow", "consistent": False, "differs": ["comparison.max_drawdown"]},
            *[{"row": r, "consistent": True, "differs": []} for r in list(pt.BACKFILL)[1:]],
        ],
    }
    text = pt.render_markdown(marked)
    label = ROWS["fast_slow"].label
    mark = "和回填检验不一致：comparison.max_drawdown"
    head = text.split("## 汇总")[0]
    assert f"- {label}：{mark}" in head
    us = text.split("## 美股大盘 1929–2010")[1].split("## QQQ 1999–2010")[0]
    qqq = text.split("## QQQ 1999–2010")[1].split("### 和回填检验")[0]
    assert f"| {label}（{mark}） |" in us
    assert us.count(mark) == 1
    assert mark not in qqq
    assert f"- {label}：不一致：comparison.max_drawdown" in text
    assert "一致。" not in head


# --- command line -----------------------------------------------------------------------------


@pytest.fixture
def cli(monkeypatch: pytest.MonkeyPatch, probe: dict[str, Any]) -> list[dict[str, Any]]:
    """The command line on the module fixture's rows and results: no data are loaded."""

    found = {(r["market"], r["row"]): r for r in probe["results"]}
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(pt, "load_data", lambda root: (DATA, GATE, LOADED, ROOT_DIR))
    monkeypatch.setattr(pt, "table_rows", lambda root_dir: list(RUN_ROWS))
    monkeypatch.setattr(
        pt, "evaluate", lambda row, market, data, loaded, gate: found[(market, row.id)]
    )
    monkeypatch.setattr(pt, "git_provenance", lambda root: PROVENANCE)
    real = pt.run_probe

    def run_probe(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(pt, "run_probe", run_probe)
    return calls


def test_check_command_prints_every_row(cli: list[dict[str, Any]]) -> None:
    result = CliRunner().invoke(pt.app, ["check"])
    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert len(lines) == 6
    assert lines[0].startswith("policy_table: us_market fixed_0.8 窗口 us_market_1929_2010 ")
    assert all(line.endswith("缺 0 天，可以") for line in lines)


def test_check_command_fails_when_a_row_cannot_run(
    cli: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    broken = pt.Row("broken", "fixed", "broken", "fixed_exposure", {"exposure": "x"})
    monkeypatch.setattr(pt, "table_rows", lambda root_dir: [broken])
    result = CliRunner().invoke(pt.app, ["check"])
    assert result.exit_code == 1
    assert "policy_table: us_market broken 算不出：ValueError" in result.output


def test_run_command_writes_the_table(cli: list[dict[str, Any]], tmp_path: Path) -> None:
    century = tmp_path / "century_trend_result.json"
    century.write_text(json.dumps(_century()), encoding="utf-8")
    out = tmp_path / "out"
    result = CliRunner().invoke(pt.app, ["run", "--out", str(out), "--century", str(century)])
    assert result.exit_code == 0, result.output
    assert cli[0]["century"] == _century() and cli[0]["provenance"] == PROVENANCE
    written = json.loads((out / pt.RESULT_JSON).read_text(encoding="utf-8"))
    assert written["century"]["status"] == "filled"
    assert sorted(p.name for p in out.iterdir()) == sorted([pt.RESULT_JSON, pt.RESULT_MD])
    lines = result.output.splitlines()
    assert "policy_table: us_market fast_slow ok" in lines
    assert any(line.startswith("policy_table: 回填检验核对 fast_slow ") for line in lines)
    assert any(line.startswith("policy_table: fast_slow：1929–2010 每年在长牛里") for line in lines)
    assert lines[-1] == f"policy_table: 完成；1871–1928 一列已填；结果目录 {out}"


def test_run_command_without_century_leaves_window_d_pending(
    cli: list[dict[str, Any]], tmp_path: Path
) -> None:
    result = CliRunner().invoke(pt.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert cli[0]["century"] is None
    assert result.output.splitlines()[-1].endswith("1871–1928 一列待乙；结果目录 " + str(tmp_path))


def test_run_command_refuses_to_overwrite(cli: list[dict[str, Any]], tmp_path: Path) -> None:
    (tmp_path / pt.RESULT_MD).write_text("old", encoding="utf-8")
    result = CliRunner().invoke(pt.app, ["run", "--out", str(tmp_path)])
    assert result.exit_code == 1
    assert "已有结果，拒绝覆盖" in result.output
    assert cli == []
    assert (tmp_path / pt.RESULT_MD).read_text(encoding="utf-8") == "old"


@pytest.mark.parametrize(
    "content",
    [
        json.dumps({**_century(), "run_id": "TRADING-2565-38"}),
        "{",
        json.dumps({"run_id": "TRADING-2565-41"}),
    ],
)
def test_run_command_refuses_an_unusable_century_file(
    cli: list[dict[str, Any]], tmp_path: Path, content: str
) -> None:
    century = tmp_path / "century.json"
    century.write_text(content, encoding="utf-8")
    result = CliRunner().invoke(
        pt.app, ["run", "--out", str(tmp_path / "out"), "--century", str(century)]
    )
    assert result.exit_code == 1
    assert "policy_table: 乙的结果不能用：" in result.output
    assert cli == [] and not (tmp_path / "out").exists()


def test_run_command_stops_when_the_check_fails(
    cli: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    short = {**DATA, "us_market": gp.cut(_us_data(start="1928-09-03"))}
    monkeypatch.setattr(pt, "load_data", lambda root: (short, GATE, LOADED, ROOT_DIR))
    result = CliRunner().invoke(pt.app, ["run", "--out", str(tmp_path / "out")])
    assert result.exit_code == 1
    assert result.output.splitlines()[-1] == "policy_table: 开跑前检查没过，停下。"
    assert cli == []


def test_a_load_failure_is_one_line(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def fail(root: Path) -> Any:
        raise tr.TrialError("no plan T0004\nmore")

    monkeypatch.setattr(pt, "load_data", fail)
    result = CliRunner().invoke(pt.app, ["run", "--out", str(tmp_path / "out")])
    assert result.exit_code == 1
    assert result.output == "policy_table: 数据载入失败：no plan T0004\n"
