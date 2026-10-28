from __future__ import annotations

import copy
import json
import math
import shutil
import statistics
from collections.abc import Mapping
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
from ai_trading_system.research_bench.data import (
    BenchData,
    from_daily_frame,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_bench.prereg import Rule
from ai_trading_system.research_bench.settings import Window
from ai_trading_system.research_probes import gate_false_pass as fp
from ai_trading_system.research_probes import gate_floor_probe as gf
from ai_trading_system.research_probes import gate_power as gp

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
V2 = fp.load_gate()
ROOT_DIR = lg.research_root(LOADED)
PREREG = PROJECT_ROOT / "research" / "_trials" / "prereg.yaml"
GATE = tr.load_gate(PREREG)
# Short windows keep the whole-run tests fast; the real ones are the gate's development windows.
WINDOWS = {
    "us_market": Window(
        window_id="us_test", role="development", start=date(1929, 1, 2), end=date(1936, 12, 31)
    ),
    "qqq": Window(
        window_id="qqq_test", role="development", start=date(1999, 3, 10), end=date(2004, 12, 31)
    ),
}
SEEDS = {"zero": 1, "effect": 1, "holdout": 1}
OLD_SEEDS = range(20261018, 20261168)
US_RULES: list[tuple[str, str, float]] = [
    (r.metric, r.op, r.value) for r in GATE.markets["us_market"].rules
]
QQQ_RULES: list[tuple[str, str, float]] = [
    (r.metric, r.op, r.value) for r in GATE.markets["qqq"].rules
]


def _qqq_data(seed: int = 3) -> BenchData:
    index = pd.bdate_range("1994-01-03", "2010-12-31", name="date")
    r = np.random.default_rng(seed).normal(0.0004, 0.015, len(index))
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
    data = from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "qlh-test"})
    return with_signal_sources(data, {}, {}, snapshot_id="sig-test")


def _us_data(end: str = "1936-12-31", seed: int = 5) -> BenchData:
    """Daily market returns from the Ken French data's first day, whose volatility changes month
    by month (as in the gate_power and gate_false_pass tests)."""

    index = pd.bdate_range("1926-07-01", end, name="date")
    rng = np.random.default_rng(seed)
    codes, uniques = pd.factorize(index.to_period("M"))
    monthly_vol = np.exp(rng.normal(math.log(0.01), 0.6, len(uniques)))
    factors = pd.DataFrame(
        {
            "Mkt-RF": rng.normal(0.0003, 1.0, len(index)) * monthly_vol[codes],
            "SMB": 0.0,
            "HML": 0.0,
            "RF": 0.0001,
        },
        index=index,
    )
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id="sig-test")
    return with_signal_sources(data, {}, {}, snapshot_id="sig-test")


US = _us_data()
QQQ = _qqq_data()
DATA = {"us_market": US, "qqq": QQQ}


def _gate(
    us_rules: list[tuple[str, str, float]] | None = None,
    qqq_rules: list[tuple[str, str, float]] | None = None,
    confidence: float | None = None,
) -> tr.TrialGate:
    markets = dict(GATE.markets)
    for market, rules in (("us_market", us_rules), ("qqq", qqq_rules)):
        if rules is not None:
            markets[market] = GATE.markets[market].model_copy(  # type: ignore[index]
                update={"rules": tuple(Rule(metric=m, op=o, value=v) for m, o, v in rules)}  # type: ignore[arg-type]
            )
    update: dict[str, Any] = {"markets": markets}
    if confidence is not None:
        update["confidence"] = confidence
    return GATE.model_copy(update=update)


def _without_seconds(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: v for k, v in row.items() if k != "seconds"} for row in gp._clean(rows)]


@pytest.fixture(scope="module")
def probe_run(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, Any], Path]:
    journal = tmp_path_factory.mktemp("gate_floor") / gf.JOURNAL_FILE
    result = gf.run_probe(
        DATA, LOADED, V2, GATE, ROOT_DIR, seeds=SEEDS, windows=WINDOWS, journal=journal
    )
    return result, journal


@pytest.fixture(scope="module")
def probe(probe_run: tuple[dict[str, Any], Path]) -> dict[str, Any]:
    return probe_run[0]


@pytest.fixture(scope="module")
def timing() -> dict[str, Any]:
    return gf.run_timing(DATA, LOADED, GATE, windows=WINDOWS)


def _old_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rows as the 2026-10-18 journal holds them: no seconds and v2's four stage-1 rules."""

    old = []
    for row in _without_seconds(rows):
        rules = [r for r in row["rules"] if (r["metric"], r["op"], r["value"]) != gf.FLOOR_RULE]
        old.append({**row, "rules": rules, "holds": all(r["holds"] for r in rules)})
    return old


def _old_result() -> dict[str, Any]:
    return {
        "probe": "gate_false_pass",
        "settings_sha256": LOADED.sha256,
        "gate_sha256": lg.chain_digest(fp.GATE_COPY.read_bytes()),
        "source": {"us_market": dict(US.source), "qqq": dict(QQQ.source)},
    }


def _equivalence(timing: dict[str, Any], baseline: list[dict[str, Any]], **change: Any) -> Any:
    kwargs: dict[str, Any] = {
        "settings_sha256": LOADED.sha256,
        "gate_sha256": lg.chain_digest(fp.GATE_COPY.read_bytes()),
        "sources": {"us_market": dict(US.source), "qqq": dict(QQQ.source)},
    }
    kwargs.update(change)
    return gf.equivalence(timing, baseline, _old_result(), **kwargs)


# --- the fixed design ----------------------------------------------------------------------------


def test_the_design_is_the_one_fixed_before_the_run() -> None:
    assert gf.RUN_ID == "TRADING-2565-82"
    assert gf.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-28:gate_floor_probe"
    assert gf.FLOORS == (0.0, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10)
    assert gf.CURRENT_FLOOR == 0.10 and gf.FIRST_SCREEN == (0.05, 0.10)
    assert gf.FLOOR_RULE == ("comparison.cvar_5pct_relative_improvement", ">=", 0.10)
    assert gf.CI_LOW_METRIC == "ci_low.cvar_5pct_relative_improvement"
    assert gf.TARGETS == (0.10, 0.13, 0.15)
    assert (gf.GAIN_LINE, gf.SELECT_LINE) == (0.10, 0.50)
    assert gf.TOLERANCE == 1e-12
    assert gf.EFFECT_NOISES == (1.5, 1.75, 2.0, 2.25, 2.5, 2.75)
    assert gf.HOLDOUT_NOISES == (1.5, 2.0, 2.5)
    assert gf.SEED_STARTS == {"zero": 20261200, "effect": 20261400, "holdout": 20261600}
    assert gf.SEGMENT == 200
    assert gf.DEFAULT_SEEDS == {"zero": 150, "effect": 80, "holdout": 40}
    assert (gf.MIN_ZERO_SEEDS, gf.TIMING_SEEDS, gf.LIMIT_MINUTES) == (100, 5, 150.0)
    assert gf.SOURCE_KEYS == ("market", "snapshot_id", "signal_snapshot_id", "qqq_series")
    assert (gf.BINS_PER_UNIT, gf.SPAN) == (100, 8.0)
    assert (gf.JOURNAL_FILE, gf.EQUIVALENCE_JSON, gf.RESULT_JSON, gf.RESULT_MD) == (
        "gate_floor_rows.jsonl",
        "gate_floor_equivalence.json",
        "gate_floor_result.json",
        "gate_floor_result.md",
    )
    # The options file's analytic estimates, pessimistic and central, for the 1929-2010 window.
    assert gf.ANALYTIC == {
        0.05: {0.10: (0.35, 0.65), 0.13: (0.49, 0.80), 0.15: (0.58, 0.86)},
        0.10: {0.10: (0.35, 0.45), 0.13: (0.49, 0.67), 0.15: (0.58, 0.78)},
    }


def test_the_families_are_the_false_pass_zero_families_then_effect_and_holdout() -> None:
    assert gf.ZERO_FAMILIES is fp.ZERO_FAMILIES
    assert list(gf.FAMILIES) == [
        "null",
        "vol_noise_3",
        "dd_noise_3",
        "vol_noise_1.5",
        "vol_noise_1.75",
        "vol_noise_2",
        "vol_noise_2.25",
        "vol_noise_2.5",
        "vol_noise_2.75",
        "dd_noise_1.5",
        "dd_noise_2",
        "dd_noise_2.5",
    ]
    for family in gf.EFFECT_FAMILIES:
        assert (family.role, family.signal) == ("effect", "volatility")
    for family in gf.HOLDOUT_FAMILIES:
        assert (family.role, family.signal) == ("holdout", "drawdown")
    assert [f.noise for f in gf.EFFECT_FAMILIES] == list(gf.EFFECT_NOISES)
    assert [f.noise for f in gf.HOLDOUT_FAMILIES] == list(gf.HOLDOUT_NOISES)
    assert all(isinstance(f, fp.Family) for f in gf.FAMILIES.values())


def test_the_seed_segments_never_overlap_each_other_or_the_10_18_seeds() -> None:
    segments = {role: set(gf.seed_list(role, gf.SEGMENT)) for role in gf.SEED_STARTS}
    roles = list(segments)
    for i, role in enumerate(roles):
        assert not segments[role] & set(OLD_SEEDS), role
        for other in roles[i + 1 :]:
            assert not segments[role] & segments[other], (role, other)
    assert gf.seed_list("effect", 3) == [20261400, 20261401, 20261402]
    assert gf.seed_list("holdout", 0) == []
    for bad in (-1, gf.SEGMENT + 1):
        with pytest.raises(ValueError, match="seed count"):
            gf.seed_list("zero", bad)


def test_seeds_are_cut_in_one_proportion_and_never_below_100() -> None:
    assert gf.scaled_seeds(150) == gf.DEFAULT_SEEDS
    assert gf.scaled_seeds(100) == {"zero": 100, "effect": 53, "holdout": 26}
    assert gf.scaled_seeds(128) == {"zero": 128, "effect": 68, "holdout": 34}
    for bad in (99, 151):
        with pytest.raises(ValueError, match="near-zero seeds"):
            gf.scaled_seeds(bad)


def test_the_design_evaluates_every_family_and_seed_on_both_stages_in_order() -> None:
    tasks = gf.design_tasks(gf.DEFAULT_SEEDS)

    assert len(tasks) == 2 * (3 * 150 + 6 * 80 + 3 * 40) == 2100
    assert tasks[:4] == [
        ("null", 20261200, "us_market"),
        ("null", 20261200, "qqq"),
        ("null", 20261201, "us_market"),
        ("null", 20261201, "qqq"),
    ]
    assert tasks[-1] == ("dd_noise_2.5", 20261639, "qqq")
    # Each effect level reuses one seed segment, as gate_power does.
    for family in gf.EFFECT_FAMILIES:
        assert sorted({s for f, s, _ in tasks if f == family.family_id}) == list(
            range(20261400, 20261480)
        )
    assert gf.design_tasks(SEEDS) == [
        (family, seed, market)
        for family, seed in (
            ("null", 20261200),
            ("vol_noise_3", 20261200),
            ("dd_noise_3", 20261200),
            *((f.family_id, 20261400) for f in gf.EFFECT_FAMILIES),
            *((f.family_id, 20261600) for f in gf.HOLDOUT_FAMILIES),
        )
        for market in ("us_market", "qqq")
    ]


def test_the_timing_tasks_are_the_first_seeds_of_the_10_18_run() -> None:
    assert gf.timing_tasks() == [
        (family, seed, market)
        for family in ("null", "vol_noise_3", "dd_noise_3")
        for seed in range(20261018, 20261023)
        for market in ("us_market", "qqq")
    ]


# --- the gate ------------------------------------------------------------------------------------


def test_the_current_gate_is_v2_plus_the_floor() -> None:
    gf.check_gates(V2, GATE)

    assert gf.load_current_gate(ROOT_DIR) == GATE
    assert US_RULES[-1] == gf.FLOOR_RULE
    assert US_RULES[:-1] == [(r.metric, r.op, r.value) for r in V2.markets["us_market"].rules]
    assert QQQ_RULES == [(r.metric, r.op, r.value) for r in V2.markets["qqq"].rules]


@pytest.mark.parametrize(
    "gate",
    [
        _gate(us_rules=[*US_RULES[:-1], (US_RULES[-1][0], ">=", 0.09)]),
        _gate(us_rules=US_RULES[:-1]),
        _gate(us_rules=[US_RULES[-1], *US_RULES[:-1]]),
        _gate(us_rules=[*US_RULES, ("comparison.max_drawdown", ">=", 0.0)]),
        _gate(us_rules=[("comparison.max_drawdown", ">=", 0.06), *US_RULES[1:]]),
        _gate(qqq_rules=[*QQQ_RULES[:-1], (QQQ_RULES[-1][0], ">=", -0.02)]),
        _gate(qqq_rules=[*QQQ_RULES, gf.FLOOR_RULE]),
        _gate(confidence=0.9),
    ],
)
def test_a_gate_that_is_not_v2_plus_the_floor_is_refused(gate: tr.TrialGate) -> None:
    with pytest.raises(ValueError):
        gf.check_gates(V2, gate)


# --- evaluation ----------------------------------------------------------------------------------


def test_every_unit_is_evaluated_on_both_stages_in_order(probe: dict[str, Any]) -> None:
    rows = probe["evaluations"]

    assert [(r["family"], r["seed"], r["market"]) for r in rows] == gf.design_tasks(SEEDS)
    for row in rows:
        assert row["role"] == gf.FAMILIES[row["family"]].role
        assert row["stage"] == {"us_market": "stage1", "qqq": "stage2"}[row["market"]]
        assert row["error"] is None
        assert isinstance(row["seconds"], float) and row["seconds"] >= 0.0


def test_rows_carry_the_current_rules_and_the_floor_rule_reads_the_point(
    probe: dict[str, Any],
) -> None:
    for row in probe["evaluations"]:
        rules = [(r["metric"], r["op"], r["value"]) for r in row["rules"]]
        assert rules == (US_RULES if row["market"] == "us_market" else QQQ_RULES)
        assert row["holds"] == all(r["holds"] for r in row["rules"])
        if row["market"] == "us_market":
            floor = row["rules"][-1]
            assert floor["observed"] == row["effect"][gp.TAIL_FIELD]
            assert floor["holds"] == (row["effect"][gp.TAIL_FIELD] >= 0.10)


@pytest.mark.parametrize("index", [0, 7, 21])
def test_a_row_is_the_false_pass_evaluation_with_the_current_rules(
    probe: dict[str, Any], index: int
) -> None:
    row = probe["evaluations"][index]
    family = gf.FAMILIES[row["family"]]
    data = DATA[row["market"]]
    targets = fp.family_targets(family, data.inputs, row["seed"])

    expected = fp.evaluate(
        data,
        targets,
        WINDOWS[row["market"]],
        LOADED,
        GATE.markets[row["market"]].rules,
        GATE.confidence,
    )

    assert gp._clean({k: row[k] for k in expected}) == gp._clean(expected)


def test_the_record_names_the_gates_the_seeds_and_the_windows(probe: dict[str, Any]) -> None:
    assert (probe["probe"], probe["run"], probe["owner_decision"]) == (
        "gate_floor_probe",
        "TRADING-2565-82",
        gf.OWNER_DECISION,
    )
    assert probe["gate_sha256"] == lg.chain_digest(fp.GATE_COPY.read_bytes())
    assert probe["prereg_sha256"] == lg.chain_digest(PREREG.read_bytes())
    assert probe["settings_sha256"] == LOADED.sha256
    assert probe["seeds"] == SEEDS and probe["seed_starts"] == gf.SEED_STARTS
    assert probe["floors"] == list(gf.FLOORS) and probe["targets"] == list(gf.TARGETS)
    assert [f["family"] for f in probe["families"]] == list(gf.FAMILIES)
    assert probe["windows"]["us_market"] == {
        "stage": "stage1",
        "window_id": "us_test",
        "start": "1929-01-02",
        "end": "1936-12-31",
    }
    assert probe["rules"]["us_market"] == [
        {"metric": m, "op": o, "value": v} for m, o, v in US_RULES
    ]
    assert (probe["cutoff"], probe["confidence"], probe["cost_bps"]) == ("2010-12-31", 0.95, 25.0)


def test_the_journal_holds_the_record_then_every_evaluation(
    probe_run: tuple[dict[str, Any], Path],
) -> None:
    result, journal = probe_run
    lines = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()]

    assert lines[0]["tasks"] == len(result["evaluations"])
    assert lines[0]["run"] == gf.RUN_ID and "evaluations" not in lines[0]
    assert lines[1:] == gp._clean(result["evaluations"])


def test_the_run_reads_its_own_summary(probe: dict[str, Any]) -> None:
    assert probe["summary"] == gf.summary(probe["evaluations"])
    assert probe["reading"] == gf.reading(probe["summary"])
    assert probe["summary"]["units"] == 12


def test_the_run_refuses_late_data_bad_seeds_and_another_gate() -> None:
    late = {"us_market": _us_data(end="2011-01-31"), "qqq": QQQ}
    with pytest.raises(ValueError, match="past"):
        gf.run_probe(late, LOADED, V2, GATE, ROOT_DIR, seeds=SEEDS, windows=WINDOWS)
    with pytest.raises(ValueError, match="past"):
        gf.run_timing(late, LOADED, GATE, windows=WINDOWS)
    for seeds in ({"zero": 0, "effect": 1, "holdout": 1}, {"zero": 1, "effect": 1}):
        with pytest.raises(ValueError, match="seed"):
            gf.run_probe(DATA, LOADED, V2, GATE, ROOT_DIR, seeds=seeds, windows=WINDOWS)
    with pytest.raises(ValueError, match="floor"):
        gf.run_probe(DATA, LOADED, V2, V2, ROOT_DIR, seeds=SEEDS, windows=WINDOWS)
    long = dict(WINDOWS)
    long["qqq"] = replace(WINDOWS["qqq"], end=date(2011, 6, 30))
    with pytest.raises(ValueError, match="past"):
        gf.run_probe(DATA, LOADED, V2, GATE, ROOT_DIR, seeds=SEEDS, windows=long)


def test_the_probe_writes_nothing_under_the_research_root(tmp_path: Path) -> None:
    root = tmp_path / "research"
    root.joinpath(tr.TRIALS_DIR).mkdir(parents=True)
    for name in ("prereg.yaml", "T0001.yaml"):
        shutil.copyfile(ROOT_DIR / tr.TRIALS_DIR / name, root / tr.TRIALS_DIR / name)
    before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))

    gf.run_probe(
        DATA, LOADED, V2, GATE, root, seeds={"zero": 1, "effect": 1, "holdout": 1}, windows=WINDOWS
    )

    assert sorted(str(p.relative_to(root)) for p in root.rglob("*")) == before


def test_workers_give_the_same_rows() -> None:
    tasks = gf.design_tasks(SEEDS)[4:8]
    payload = {"data": DATA, "windows": WINDOWS, "loaded": LOADED, "gate": GATE}
    one: list[dict[str, Any]] = []
    two: list[dict[str, Any]] = []

    gf.evaluate_tasks(tasks, payload, lambda done, row: one.append(row))
    gf.evaluate_tasks(tasks, payload, lambda done, row: two.append(row), workers=2)

    assert _without_seconds(two) == _without_seconds(one)
    assert [(r["family"], r["seed"], r["market"]) for r in one] == tasks


def test_a_refused_evaluation_is_an_error_row_and_the_reading_is_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("no targets\nsecond line")

    monkeypatch.setattr(fp, "family_targets", refuse)
    rows: list[dict[str, Any]] = []
    tasks = gf.design_tasks(SEEDS)[:2]

    gf.evaluate_tasks(
        tasks,
        {"data": DATA, "windows": WINDOWS, "loaded": LOADED, "gate": GATE},
        lambda done, row: rows.append(row),
    )

    assert [row["error"] for row in rows] == ["ValueError: no targets"] * 2
    assert not any(row["holds"] for row in rows)
    found = gf.summary(rows)
    assert found["errors"] == 1
    assert gf.reading(found)["verdict"] == "incomplete"


# --- units and floors ----------------------------------------------------------------------------


def _pair(
    family: str,
    seed: int,
    point: float | None,
    *,
    core: bool = True,
    ci_low: float = 0.01,
    stage2: bool = True,
    error: str | None = None,
) -> list[dict[str, Any]]:
    """Stage-1 and stage-2 rows as the probe writes them, with v2's rules holding or not."""

    rows = []
    for stage, market, rules in (("stage1", "us_market", US_RULES), ("stage2", "qqq", QQQ_RULES)):
        items = []
        for metric, op, value in rules:
            if (metric, op, value) == gf.FLOOR_RULE:
                observed, holds = point, point is not None and point >= value
            elif metric == gf.CI_LOW_METRIC:
                observed, holds = ci_low, core
            else:
                observed, holds = 0.0, core if stage == "stage1" else stage2
            items.append(
                {"metric": metric, "op": op, "value": value, "observed": observed, "holds": holds}
            )
        rows.append(
            {
                "family": family,
                "role": gf.FAMILIES[family].role,
                "seed": seed,
                "market": market,
                "stage": stage,
                "effect": {gp.TAIL_FIELD: point if stage == "stage1" else -1.0},
                "rules": [] if error else items,
                "holds": False if error else all(item["holds"] for item in items),
                "error": error if stage == "stage1" else None,
            }
        )
    return rows


def test_a_unit_reads_v2_the_current_gate_the_point_and_the_interval() -> None:
    rows = [
        *_pair("null", 1, 0.12, ci_low=0.02),
        *_pair("null", 2, 0.07),
        *_pair("null", 3, 0.20, core=False),
        *_pair("null", 4, 0.20, stage2=False),
        *_pair("null", 5, 0.20, error="ValueError: x"),
        _pair("null", 6, 0.20)[0],
    ]

    items = {unit["seed"]: unit for unit in gf.units(rows)}

    assert items[1] == {
        "family": "null",
        "role": "zero",
        "seed": 1,
        "point": 0.12,
        "ci_low": 0.02,
        "v2": True,
        "current": True,
        "error": False,
    }
    assert (items[2]["v2"], items[2]["current"]) == (True, False)
    assert (items[3]["v2"], items[3]["current"]) == (False, False)
    assert (items[4]["v2"], items[4]["current"]) == (False, False)
    for seed in (5, 6):
        assert items[seed]["error"] is True
        assert (items[seed]["point"], items[seed]["v2"]) == (None, False)
    assert gf.summary(rows)["errors"] == 2


def test_floor_zero_is_v2_and_a_floor_needs_the_point() -> None:
    unit = {"error": False, "v2": True, "point": -0.30}
    assert gf.passes(unit, 0.0) is True
    assert gf.passes(unit, 0.05) is False
    assert gf.passes({**unit, "point": 0.05}, 0.05) is True
    assert gf.passes({**unit, "point": 0.0499999}, 0.05) is False
    assert gf.passes({**unit, "point": None}, 0.0) is True
    assert gf.passes({**unit, "point": None}, 0.05) is False
    assert gf.passes({**unit, "v2": False, "point": 0.5}, 0.0) is False
    assert gf.passes({**unit, "error": True, "point": 0.5}, 0.10) is False


def test_the_pass_sets_are_nested(probe: dict[str, Any]) -> None:
    synthetic = gf.units([row for i in range(40) for row in _pair("vol_noise_3", i, i / 200)])
    for items in (synthetic, gf.units(probe["evaluations"])):
        sets = [
            {(u["family"], u["seed"]) for u in items if gf.passes(u, floor)} for floor in gf.FLOORS
        ]
        for low, high in zip(sets, sets[1:], strict=False):
            assert high <= low
    counts = [row["passes"] for row in gf.floor_rows(synthetic)]
    assert counts == sorted(counts, reverse=True)
    assert counts[0] == 40 and counts[1] == 30 and counts[-1] == 20


def _zero_units(total: int, passing: int) -> list[dict[str, Any]]:
    families = ["null", "vol_noise_3", "dd_noise_3"]
    return [
        {
            "family": families[i % 3],
            "role": "zero",
            "seed": i,
            "point": 0.10 if i < passing else 0.0,
            "ci_low": 0.0,
            "v2": True,
            "current": i < passing,
            "error": False,
        }
        for i in range(total)
    ]


@pytest.mark.parametrize(
    ("total", "passing", "holds"),
    [(450, 4, True), (450, 5, False), (300, 2, True), (300, 3, False)],
)
def test_a_floor_holds_when_the_wilson_upper_bound_is_at_most_2_5_percent(
    total: int, passing: int, holds: bool
) -> None:
    rows = {row["floor"]: row for row in gf.floor_rows(_zero_units(total, passing))}

    top = rows[0.10]
    assert (top["passes"], top["total"]) == (passing, total)
    assert (top["low"], top["high"]) == fp.wilson(passing, total)
    assert top["holds"] is holds
    assert rows[0.0]["passes"] == total and rows[0.0]["holds"] is False
    assert sum(item["passes"] for item in top["families"].values()) == passing
    assert top["families"]["null"]["total"] == total // 3


def test_an_upper_bound_on_the_line_holds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fp, "USABLE_LINE", fp.wilson(4, 450)[1])

    rows = {row["floor"]: row for row in gf.floor_rows(_zero_units(450, 4))}

    assert rows[0.10]["high"] == fp.USABLE_LINE
    assert rows[0.10]["holds"] is True


def test_an_effect_family_row(probe: dict[str, Any]) -> None:
    items = [
        {"family": "vol_noise_2", "role": "effect", "seed": 1, "point": 0.20, "ci_low": 0.02,
         "v2": True, "current": True, "error": False},
        {"family": "vol_noise_2", "role": "effect", "seed": 2, "point": 0.08, "ci_low": -0.04,
         "v2": True, "current": False, "error": False},
        {"family": "vol_noise_2", "role": "effect", "seed": 3, "point": 0.12, "ci_low": None,
         "v2": False, "current": False, "error": False},
        {"family": "vol_noise_2", "role": "effect", "seed": 4, "point": None, "ci_low": None,
         "v2": False, "current": False, "error": True},
    ]  # fmt: skip

    (row,) = gf.family_rows(items, [gf.FAMILIES["vol_noise_2"]])

    assert (row["family"], row["noise"], row["seeds"], row["errors"]) == ("vol_noise_2", 2.0, 4, 1)
    assert row["effect"] == pytest.approx(0.12)
    assert row["sd"] == pytest.approx(statistics.stdev([0.20, 0.08, 0.12]))
    assert row["se"] == pytest.approx(statistics.median([0.18, 0.12]) / fp.WILSON_Z)
    assert row["pass"] == {
        "0.00": 0.5,
        "0.05": 0.5,
        "0.06": 0.5,
        "0.07": 0.5,
        "0.08": 0.5,
        "0.09": 0.25,
        "0.10": 0.25,
    }
    empty = gf.family_rows([], [gf.FAMILIES["dd_noise_2"]])[0]
    assert (empty["seeds"], empty["effect"], empty["sd"], empty["se"]) == (0, None, None, None)
    assert set(empty["pass"].values()) == {None}


# --- detection -----------------------------------------------------------------------------------


def test_interpolation_is_linear_between_neighbours_and_never_extrapolates() -> None:
    points = [(0.17, 0.9), (0.05, 0.1), (0.11, 0.5), (None, 1.0), (0.08, None)]

    assert gf.interpolate(points, 0.11) == 0.5
    assert gf.interpolate(points, 0.05) == 0.1
    assert gf.interpolate(points, 0.17) == 0.9
    assert gf.interpolate(points, 0.14) == pytest.approx(0.7)
    assert gf.interpolate(points, 0.08) == pytest.approx(0.3)
    assert gf.interpolate(points, 0.0499) is None
    assert gf.interpolate(points, 0.1701) is None
    assert gf.interpolate([], 0.1) is None
    # Equal effects: the first one given wins.
    assert gf.interpolate([(0.10, 0.2), (0.10, 0.4)], 0.10) == 0.2


def _effect_rows(table: list[tuple[float | None, dict[float, float], float | None]]) -> Any:
    return [
        {
            "family": f"f{i}",
            "seeds": 80,
            "effect": effect,
            "se": se,
            "pass": {gf.floor_key(f): shares.get(f, 0.0) for f in gf.FLOORS},
        }
        for i, (effect, shares, se) in enumerate(table)
    ]


def test_detection_reads_each_floor_at_10_13_and_15_percent() -> None:
    rows = _effect_rows(
        [
            (0.17, {0.0: 1.0, 0.05: 0.9, 0.10: 0.8}, 0.04),
            (0.12, {0.0: 0.8, 0.05: 0.6, 0.10: 0.5}, 0.04),
            (0.06, {0.0: 0.4, 0.05: 0.2, 0.10: 0.0}, 0.04),
        ]
    )

    table = gf.detection(rows)

    assert list(table) == ["0.00", "0.05", "0.06", "0.07", "0.08", "0.09", "0.10"]
    assert list(table["0.10"]) == ["0.10", "0.13", "0.15"]
    assert table["0.10"]["0.10"] == pytest.approx(0.0 + 0.5 * (0.04 / 0.06))
    assert table["0.05"]["0.13"] == pytest.approx(0.6 + 0.3 * (0.01 / 0.05))
    assert table["0.00"]["0.15"] == pytest.approx(0.8 + 0.2 * (0.03 / 0.05))
    assert gf.detection(rows[1:])["0.10"]["0.15"] is None


def test_the_other_rules_are_binned_by_one_point_of_the_effect_family_points() -> None:
    def unit(role: str, point: float | None, v2: bool) -> dict[str, Any]:
        return {"family": "x", "role": role, "seed": 0, "point": point, "v2": v2, "error": False}

    items = [
        unit("effect", 0.071, True),
        unit("effect", 0.079, False),
        unit("effect", 0.12, True),
        unit("effect", -0.005, True),
        unit("effect", None, True),
        unit("zero", 0.071, False),
        unit("holdout", 0.30, True),
    ]

    assert gf.other_rules(items) == {-1: 1.0, 7: 0.5, 12: 1.0}


def test_the_path_measure_is_the_normal_mass_above_the_floor_times_the_other_rules() -> None:
    rows = _effect_rows([(0.05, {}, 0.03), (0.15, {}, 0.05)])
    ones = {k: 1.0 for k in range(-100, 100)}
    se = 0.03 + 0.02 * (0.10 - 0.05) / 0.10

    for floor in (0.05, 0.08, 0.10):
        expected = 1.0 - statistics.NormalDist(0.10, se).cdf(floor)
        assert gf.path_detection(rows, ones, floor, 0.10) == pytest.approx(expected, abs=1e-9)
    assert gf.path_detection(rows, ones, 0.0, 0.13) == pytest.approx(1.0, abs=1e-9)
    # The other rules weigh each bin; an empty bin takes the nearest one, the lower on a tie.
    half = {k: (1.0 if k >= 10 else 0.0) for k in range(-100, 100)}
    normal = statistics.NormalDist(0.10, se)
    assert gf.path_detection(rows, half, 0.05, 0.10) == pytest.approx(
        1.0 - normal.cdf(0.10), abs=1e-9
    )
    sparse = {8: 0.2, 12: 0.6}
    assert gf._nearest(sparse, 10) == 0.2
    assert gf._nearest(sparse, 11) == 0.6
    assert gf._nearest(sparse, -50) == 0.2
    assert gf.path_detection(rows, {}, 0.05, 0.10) is None
    assert gf.path_detection(rows, ones, 0.05, 0.20) is None
    assert (
        gf.path_detection(_effect_rows([(0.05, {}, None), (0.15, {}, None)]), ones, 0.05, 0.1)
        is None
    )


# --- reading -------------------------------------------------------------------------------------


def _summary(
    holds: set[float],
    p10: Mapping[float, float | None],
    *,
    path: Mapping[float, float | None] | None = None,
    held: Mapping[float, float | None] | None = None,
    errors: int = 0,
    mismatches: list[str] | None = None,
    zero_total: int = 450,
    seeds: int = 80,
) -> dict[str, Any]:
    def table(values: Mapping[float, float | None]) -> dict[str, dict[str, float | None]]:
        return {
            gf.floor_key(f): {"0.10": values.get(f), "0.13": None, "0.15": None} for f in gf.FLOORS
        }

    return {
        "errors": errors,
        "gate_mismatches": mismatches or [],
        "floors": [
            {"floor": f, "passes": 0, "total": zero_total, "holds": f in holds} for f in gf.FLOORS
        ],
        "effect": [{"family": "vol_noise_2", "seeds": seeds}],
        "detection": table(p10),
        "path": table(path if path is not None else p10),
        "holdout_detection": table(held if held is not None else p10),
    }


ALL = {0.05, 0.06, 0.07, 0.08, 0.09, 0.10}
FROM_7 = {0.07, 0.08, 0.09, 0.10}
P10 = {f: 0.45 for f in gf.FLOORS}


@pytest.mark.parametrize(
    ("summary", "verdict"),
    [
        (_summary(FROM_7, {**P10, 0.07: 0.55}), "lower_to_f"),
        (_summary(FROM_7, {**P10, 0.07: 0.549}), "small_gain"),
        # 0.3 - 0.2 is just under 0.10 in floating point: a gain on the line is lower_to_f.
        (_summary(FROM_7, {**{f: 0.2 for f in gf.FLOORS}, 0.07: 0.3}), "lower_to_f"),
        (_summary({0.10}, P10), "keep_10"),
        (_summary(set(), P10), "floor10_loose"),
        (_summary(FROM_7, P10, errors=1), "incomplete"),
        (_summary(FROM_7, P10, mismatches=["null 20261200"]), "incomplete"),
        (_summary(FROM_7, P10, zero_total=0), "incomplete"),
        (_summary(FROM_7, {**P10, 0.10: None}), "incomplete"),
        (_summary(FROM_7, P10, seeds=0), "incomplete"),
        (_summary(set(), P10, errors=1), "incomplete"),
    ],
)
def test_the_verdicts_in_their_fixed_order(summary: dict[str, Any], verdict: str) -> None:
    assert gf.reading(summary)["verdict"] == verdict


def test_f_star_is_the_lowest_holding_floor_and_the_next_one_is_printed() -> None:
    read = gf.reading(_summary(FROM_7, {**P10, 0.07: 0.60}))
    assert (read["fstar"], read["fnext"]) == (0.07, 0.08)
    assert read["p10_current"] == 0.45 and read["p10_fstar"] == 0.60
    assert read["gain"] == pytest.approx(0.15)
    assert read["select"] is True

    assert gf.reading(_summary(FROM_7, {**P10, 0.07: 0.50}))["select"] is True
    read = gf.reading(_summary(ALL, {**P10, 0.05: 0.49}))
    assert (read["fstar"], read["fnext"], read["select"]) == (0.05, 0.06, False)
    read = gf.reading(_summary({0.10}, P10))
    assert (read["fstar"], read["fnext"], read["gain"]) == (0.10, None, 0.0)
    read = gf.reading(_summary(set(), P10))
    assert (read["fstar"], read["fnext"], read["gain"], read["select"]) == (None, None, None, None)
    # Line 0 is printed only: v2 holding never makes f* zero.
    assert gf.reading(_summary({0.0, *FROM_7}, P10))["fstar"] == 0.07


def test_the_two_measures_and_the_holdout_are_flagged_only_across_the_line() -> None:
    seed = {**P10, 0.07: 0.60}
    read = gf.reading(_summary(FROM_7, seed, path={**P10, 0.07: 0.50}))
    assert read["verdict"] == "lower_to_f"
    assert read["measures_differ"] is True and read["path_gain"] == pytest.approx(0.05)
    assert gf.reading(_summary(FROM_7, seed, path={**P10, 0.07: 0.56}))["measures_differ"] is False
    read = gf.reading(_summary(FROM_7, seed, held={**P10, 0.07: 0.46}))
    assert read["holdout_differ"] is True and read["holdout_gain"] == pytest.approx(0.01)
    assert gf.reading(_summary(FROM_7, seed, held={**P10, 0.07: None}))["holdout_differ"] is False
    # An incomplete reading flags nothing, and keep_10 has nothing to compare.
    read = gf.reading(_summary(FROM_7, seed, path={**P10, 0.07: 0.50}, held=P10, errors=1))
    assert read["verdict"] == "incomplete"
    assert (read["measures_differ"], read["holdout_differ"]) == (False, False)
    read = gf.reading(_summary({0.10}, P10, path={**P10, 0.10: 0.9}))
    assert (read["measures_differ"], read["holdout_differ"]) == (False, False)


# --- timing and equivalence ----------------------------------------------------------------------


def test_the_timing_rows_are_the_30_old_seed_evaluations(timing: dict[str, Any]) -> None:
    rows = timing["rows"]

    assert timing["tasks"] == 30 and len(rows) == 30
    assert [(r["family"], r["seed"], r["market"]) for r in rows] == gf.timing_tasks()
    assert timing["elapsed_seconds"] >= sum(0.0 for _ in rows)
    assert all(r["error"] is None and r["seconds"] >= 0.0 for r in rows)


def test_the_rerun_equals_the_same_rows_written_the_10_18_way(timing: dict[str, Any]) -> None:
    check = _equivalence(timing, _old_rows(timing["rows"]))

    assert check["ok"] is True
    assert (check["rows"], check["expected_rows"]) == (30, 30)
    assert check["problems"] == [] and check["differ"] == []
    assert check["checks"]["source.us_market.snapshot_id"] == ["sig-test", "sig-test"]
    assert check["checks"]["source.qqq.snapshot_id"] == ["qlh-test", "qlh-test"]


def _changed(rows: list[dict[str, Any]], change: str) -> list[dict[str, Any]]:
    old: list[dict[str, Any]] = json.loads(json.dumps(rows))
    first = old[0]
    if change == "effect_far":
        first["effect"][gp.TAIL_FIELD] += 2e-12
    elif change == "effect_near":
        first["effect"][gp.TAIL_FIELD] += 5e-13
    elif change == "observed":
        first["rules"][1]["observed"] += 1e-9
    elif change == "holds":
        first["rules"][0]["holds"] = not first["rules"][0]["holds"]
    elif change == "extra_rule":
        first["rules"].append(
            {"metric": "comparison.max_drawdown", "op": ">", "value": 0.5, "observed": 0.0,
             "holds": False}
        )  # fmt: skip
    elif change == "window":
        first["end"] = "1936-12-30"
    elif change == "exposure":
        first["average_exposure"] += 1e-6
    elif change == "error":
        first["error"] = "ValueError: x"
    elif change == "missing":
        old = old[1:]
    return old


@pytest.mark.parametrize(
    ("change", "fields"),
    [
        ("effect_far", ["effect.cvar_5pct_relative_improvement"]),
        ("effect_near", []),
        ("observed", ["comparison.max_drawdown >= 0.05"]),
        ("holds", ["ci_low.cvar_5pct_relative_improvement > 0"]),
        ("extra_rule", ["comparison.max_drawdown > 0.5"]),
        ("window", ["end"]),
        ("exposure", ["average_exposure"]),
        ("error", ["error"]),
        ("missing", ["row"]),
    ],
)
def test_a_difference_from_the_10_18_rows_is_named(
    timing: dict[str, Any], change: str, fields: list[str]
) -> None:
    check = _equivalence(timing, _changed(_old_rows(timing["rows"]), change))

    assert [p["field"] for p in check["problems"]] == fields
    assert all(p["row"] == "null 20261018 us_market" for p in check["problems"])
    assert check["ok"] is (not fields)


def test_settings_gate_and_snapshot_ids_are_compared(timing: dict[str, Any]) -> None:
    rows = _old_rows(timing["rows"])
    other = {"us_market": {**US.source, "snapshot_id": "sig-other"}, "qqq": dict(QQQ.source)}

    assert _equivalence(timing, rows, settings_sha256="x")["differ"] == ["settings_sha256"]
    assert _equivalence(timing, rows, gate_sha256="x")["differ"] == ["gate_sha256"]
    check = _equivalence(timing, rows, sources=other)
    assert check["differ"] == ["source.us_market.snapshot_id"] and check["ok"] is False
    # Paths and checks outside SOURCE_KEYS may change with the data root; they are not compared.
    moved = {"us_market": {**US.source, "path": "D:/x"}, "qqq": {**QQQ.source, "check": "WARN"}}
    assert _equivalence(timing, rows, sources=moved)["ok"] is True
    short = {**timing, "rows": timing["rows"][:-1]}
    assert _equivalence(short, rows)["ok"] is False


def test_a_10_18_baseline_written_by_gate_false_pass_is_equivalent(
    timing: dict[str, Any], tmp_path: Path
) -> None:
    journal = tmp_path / fp.JOURNAL_FILE
    old = fp.run_probe(
        DATA, LOADED, V2, ROOT_DIR, seeds=5, control_seeds=0, windows=WINDOWS, journal=journal
    )
    # gate_false_pass's run command adds the data sources after run_probe.
    old["source"] = {"us_market": dict(US.source), "qqq": dict(QQQ.source)}
    fp.write_result(old, tmp_path)
    rows, result = gf.read_baseline(tmp_path)

    check = gf.equivalence(
        timing,
        rows,
        result,
        settings_sha256=LOADED.sha256,
        gate_sha256=lg.chain_digest(fp.GATE_COPY.read_bytes()),
        sources={"us_market": dict(US.source), "qqq": dict(QQQ.source)},
    )

    assert check["ok"] is True and check["problems"] == [] and check["differ"] == []


def test_a_unit_passing_v2_below_the_floor_equals_its_10_18_row(timing: dict[str, Any]) -> None:
    # 10-18's timing run had one of 15 units passing v2 on both stages, below 10%: its old row
    # holds and the rerun row does not. Only the old rules are compared, never the row's holds.
    row = copy.deepcopy(next(r for r in timing["rows"] if r["market"] == "us_market"))
    for rule in row["rules"][:-1]:
        rule["holds"] = True
    row["rules"][-1]["holds"] = False
    row["holds"] = False
    old = _old_rows([row])

    assert old[0]["holds"] is True
    assert gf.compare_rows([row], old) == []


def test_the_10_18_journal_is_read_without_its_header(tmp_path: Path) -> None:
    rows = [{"family": "null", "seed": 1, "market": "qqq"}]
    tmp_path.joinpath(fp.JOURNAL_FILE).write_text(
        json.dumps({"probe": "gate_false_pass", "tasks": 1}) + "\n" + json.dumps(rows[0]) + "\n\n",
        encoding="utf-8",
    )
    tmp_path.joinpath(fp.RESULT_JSON).write_text(json.dumps(_old_result()), encoding="utf-8")

    assert gf.read_baseline(tmp_path) == (rows, _old_result())
    tmp_path.joinpath(fp.RESULT_JSON).write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError):
        gf.read_baseline(tmp_path)


@pytest.mark.parametrize(
    ("seconds", "workers", "seeds", "reduced"),
    [
        (27.0, 8, {"zero": 150, "effect": 80, "holdout": 40}, False),
        (40.0, 8, {"zero": 128, "effect": 68, "holdout": 34}, True),
        (100.0, 8, {"zero": 100, "effect": 53, "holdout": 26}, True),
        (3.0, 0, {"zero": 150, "effect": 80, "holdout": 40}, False),
    ],
)
def test_the_timing_sets_the_seeds(
    seconds: float, workers: int, seeds: dict[str, int], reduced: bool
) -> None:
    rows = [{"seconds": seconds}] * 29 + [{"seconds": seconds}]

    plan = gf.plan_seeds({"rows": rows}, workers)

    estimate = seconds * 2100 / max(1, workers) / 60
    assert plan["estimate_minutes"] == pytest.approx(estimate)
    assert plan["mean_seconds"] == pytest.approx(seconds)
    assert (plan["seeds"], plan["reduced"], plan["full_tasks"]) == (seeds, reduced, 2100)
    assert plan["tasks"] == len(gf.design_tasks(seeds))
    assert plan["planned_minutes"] == pytest.approx(seconds * plan["tasks"] / max(1, workers) / 60)
    with pytest.raises(ValueError):
        gf.plan_seeds({"rows": []}, 8)


# --- output and command --------------------------------------------------------------------------


def test_the_result_is_written_as_json_and_markdown(probe: dict[str, Any], tmp_path: Path) -> None:
    gf.write_result(probe, tmp_path)

    data = json.loads(tmp_path.joinpath(gf.RESULT_JSON).read_text(encoding="utf-8"))
    assert data["reading"] == gp._clean(probe["reading"])
    assert len(data["evaluations"]) == len(probe["evaluations"])
    text = tmp_path.joinpath(gf.RESULT_MD).read_text(encoding="utf-8")
    assert text.startswith("# 试跑门的线能降到多低（TRADING-2565-82）\n")
    assert f"- 判定：{probe['reading']['verdict']}；" in text
    for sentence in (
        f"依据 `{gf.OWNER_DECISION}`（第 6 期章程第 22 节）。",
        gf.NOT_A_TRIAL,
        gf.SEED_MEASURE,
        f"P10（{gf.ANCHOR_NOTE}）",
        gf.PATH_MEASURE,
        gf.LIMITS,
        "## 解析估算（不同口径，只作参照）",
        "## 回撤族（留出，不参与挑线和插值）",
        "## 计时和等价核对",
        "trial_gate_v2.yaml",
    ):
        assert sentence in text, sentence
    for family in gf.FAMILIES:
        assert f"| {family} |" in text
    assert text.endswith("\nproduction_effect=none；broker_action=none。\n")


def test_the_first_screen_prints_5_and_10_then_f_star_and_the_next_floor(
    probe: dict[str, Any],
) -> None:
    result = dict(probe)
    result["reading"] = {
        **probe["reading"],
        "verdict": "lower_to_f",
        "fstar": 0.07,
        "fnext": 0.08,
        "p10_fstar": 0.6,
        "gain": 0.15,
        "path_gain": 0.05,
        "measures_differ": True,
        "select": True,
    }

    text = gf.render_markdown(result)
    first = text.split("\n\n")[2].splitlines()

    assert [line.split(" | ")[0] for line in first[2:]] == ["| 0.05", "| 0.10", "| 0.07", "| 0.08"]
    assert (
        "- 两种口径判定不同：种子口径多 +15.0 个点，路径口径多 +5.0 个点；11-20 续期卡两个数都写。"
        in text
    )
    assert (
        "- f* = 0.07，f* 上面一格 0.08；P10(f*) 60.0%，到了 50%（第 20 节选方向的线，不是门的线）。"
        in text
    )
    assert gf.VERDICT_TEXT["lower_to_f"] in text


def _baseline_dir(tmp_path: Path, rows: list[dict[str, Any]]) -> Path:
    directory = tmp_path / "falsepass-01"
    directory.mkdir()
    lines = [json.dumps({"probe": "gate_false_pass", "tasks": len(rows)})]
    lines += [json.dumps(row) for row in rows]
    directory.joinpath(fp.JOURNAL_FILE).write_text("\n".join(lines) + "\n", encoding="utf-8")
    directory.joinpath(fp.RESULT_JSON).write_text(json.dumps(_old_result()), encoding="utf-8")
    return directory


def _patch_loading(monkeypatch: pytest.MonkeyPatch, timing: dict[str, Any]) -> None:
    monkeypatch.setattr(fp, "load_datasets", lambda root: (DATA, V2, LOADED, ROOT_DIR))
    monkeypatch.setattr(gf, "run_timing", lambda *args, **kwargs: timing)


def test_the_command_checks_the_old_seeds_then_runs_the_probe(
    probe: dict[str, Any], timing: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append({"args": args, **kwargs})
        kwargs["journal"].write_text("{}\n", encoding="utf-8")
        return dict(probe)

    _patch_loading(monkeypatch, timing)
    monkeypatch.setattr(gf, "run_probe", fake_run)
    baseline = _baseline_dir(tmp_path, _old_rows(timing["rows"]))
    out = tmp_path / "gate_floor"

    result = CliRunner().invoke(
        gf.app, ["run", "--out", str(out), "--baseline", str(baseline), "--workers", "8"]
    )

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert calls[0]["args"][1:4] == (LOADED, V2, GATE)
    assert calls[0]["seeds"] == gf.DEFAULT_SEEDS and calls[0]["workers"] == 8
    assert calls[0]["journal"] == out / gf.JOURNAL_FILE
    check = json.loads(out.joinpath(gf.EQUIVALENCE_JSON).read_text(encoding="utf-8"))
    assert check["equivalence"]["ok"] is True and len(check["timing"]["rows"]) == 30
    assert check["plan"]["seeds"] == gf.DEFAULT_SEEDS
    data = json.loads(out.joinpath(gf.RESULT_JSON).read_text(encoding="utf-8"))
    assert data["source"] == {"us_market": US.source, "qqq": QQQ.source}
    assert data["equivalence"]["ok"] is True and data["plan"]["workers"] == 8
    assert "gate_floor_probe: 等价核对通过" in result.output
    assert f"gate_floor_probe: 判定 {probe['reading']['verdict']}" in result.output
    text = out.joinpath(gf.RESULT_MD).read_text(encoding="utf-8")
    assert "- 10-18 的旧种子 20261018–20261022 重跑 30 行，逐行比：不同 0 处；" in text

    again = CliRunner().invoke(gf.app, ["run", "--out", str(out), "--baseline", str(baseline)])
    assert again.exit_code == 1 and "已有结果，拒绝覆盖" in again.output
    assert len(calls) == 1


def test_the_command_stops_before_the_full_run_when_a_row_differs(
    timing: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[Any] = []
    _patch_loading(monkeypatch, timing)
    monkeypatch.setattr(gf, "run_probe", lambda *args, **kwargs: calls.append(kwargs))
    baseline = _baseline_dir(tmp_path, _changed(_old_rows(timing["rows"]), "effect_far"))
    out = tmp_path / "gate_floor"

    result = CliRunner().invoke(gf.app, ["run", "--out", str(out), "--baseline", str(baseline)])

    assert result.exit_code == 1
    assert "和 10-18 不同" in result.output and "不跑全量" in result.output
    assert calls == []
    check = json.loads(out.joinpath(gf.EQUIVALENCE_JSON).read_text(encoding="utf-8"))
    assert check["equivalence"]["ok"] is False
    assert not out.joinpath(gf.JOURNAL_FILE).exists()
    assert not out.joinpath(gf.RESULT_JSON).exists()


def test_the_command_stops_when_the_baseline_data_or_gate_do_not_load(
    timing: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "x"
    missing = CliRunner().invoke(
        gf.app, ["run", "--out", str(out), "--baseline", str(tmp_path / "none")]
    )
    assert missing.exit_code == 1 and "读不了 10-18 的结果" in missing.output

    baseline = _baseline_dir(tmp_path, _old_rows(timing["rows"]))

    def refuse(root: Path) -> Any:
        raise ValueError("snapshot not checked")

    monkeypatch.setattr(fp, "load_datasets", refuse)
    failed = CliRunner().invoke(gf.app, ["run", "--out", str(out), "--baseline", str(baseline)])
    assert failed.exit_code == 1 and "数据或门载入失败：snapshot not checked" in failed.output

    monkeypatch.setattr(fp, "load_datasets", lambda root: (DATA, V2, LOADED, ROOT_DIR))
    monkeypatch.setattr(gf, "load_current_gate", lambda root_dir: V2)
    other = CliRunner().invoke(gf.app, ["run", "--out", str(out), "--baseline", str(baseline)])
    assert other.exit_code == 1 and "数据或门载入失败" in other.output
    assert not out.exists()
