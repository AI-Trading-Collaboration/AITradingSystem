from __future__ import annotations

import json
import math
import shutil
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
from ai_trading_system.research_bench.falsify import FalsificationInputs, falsify
from ai_trading_system.research_bench.metrics import compare, summarize
from ai_trading_system.research_bench.prereg import Rule, evaluate_rules
from ai_trading_system.research_bench.run import PRIMARY_BENCHMARK, benchmark_simulations
from ai_trading_system.research_bench.settings import Window
from ai_trading_system.research_bench.simulate import simulate
from ai_trading_system.research_probes import gate_false_pass as fp
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import ruler_power as rp

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
GATE = fp.load_gate()
ROOT_DIR = lg.research_root(LOADED)
PREREG = PROJECT_ROOT / "research" / "_trials" / "prereg.yaml"
# Short windows keep the whole-run tests fast; the real ones are the gate's development windows.
WINDOWS = {
    "us_market": Window(
        window_id="us_test", role="development", start=date(1929, 1, 2), end=date(1936, 12, 31)
    ),
    "qqq": Window(
        window_id="qqq_test", role="development", start=date(1999, 3, 10), end=date(2004, 12, 31)
    ),
}
SEEDS, CONTROL_SEEDS = 3, 2


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
    by month (as in the gate_power tests)."""

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


@pytest.fixture(scope="module")
def probe_run(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, Any], Path]:
    journal = tmp_path_factory.mktemp("false_pass") / fp.JOURNAL_FILE
    result = fp.run_probe(
        DATA,
        LOADED,
        GATE,
        ROOT_DIR,
        seeds=SEEDS,
        control_seeds=CONTROL_SEEDS,
        windows=WINDOWS,
        journal=journal,
    )
    return result, journal


@pytest.fixture(scope="module")
def probe(probe_run: tuple[dict[str, Any], Path]) -> dict[str, Any]:
    return probe_run[0]


# --- the gate and the fixed design ---------------------------------------------------------------


def test_the_gate_copy_is_the_trial_gate_byte_for_byte() -> None:
    # The probe measures the gate trials use now: research/_trials/prereg.yaml after the two-stage
    # ruler (TRADING-2565-20) and the Treasury reserve types (TRADING-2565-21).
    assert fp.GATE_COPY.read_bytes() == PREREG.read_bytes()
    assert lg.chain_digest(fp.GATE_COPY.read_bytes()) == (
        "d9de5cc21bd1f02085eab72b8d87bb8715db2f62dbe4d7f1105dce44cad7380f"
    )
    assert GATE == tr.load_gate(PREREG)
    assert GATE.owner_decision == "owner_decision:TRADING-2565:2026-10-17:ruler_40y"
    assert GATE.confidence == 0.95
    assert [(r.metric, r.op, r.value) for r in GATE.markets["us_market"].rules] == [
        ("ci_low.cvar_5pct_relative_improvement", ">", 0.0),
        ("comparison.max_drawdown", ">=", 0.05),
        ("comparison.geometric_annual_return", ">=", -0.01),
        ("cost_25.geometric_annual_return", ">=", -0.01),
    ]
    assert [(r.metric, r.op, r.value) for r in GATE.markets["qqq"].rules] == [
        ("ci_high.cvar_5pct_relative_improvement", ">", 0.0),
        ("comparison.geometric_annual_return", ">=", -0.01),
    ]


def test_the_design_is_the_one_fixed_before_any_run() -> None:
    assert fp.RUN_ID == "TRADING-2565-22"
    assert fp.SEED_START == 20261018
    assert (fp.DEFAULT_SEEDS, fp.DEFAULT_CONTROL_SEEDS) == (150, 20)
    assert fp.WARMUP_MONTHS == 24
    assert (fp.CONFIDENCE, fp.COST_BPS) == (0.95, 25.0)
    assert (fp.USABLE_LINE, fp.REDUCED_LINE, fp.FAMILY_LINE) == (0.025, 0.05, 0.10)
    assert fp.WILSON_Z == pytest.approx(1.959963984540054)
    assert fp.STAGES == (("stage1", "us_market"), ("stage2", "qqq"))
    assert fp.SECTIONS == ("comparison", "cost_25", "ci_low", "ci_high")
    assert [(f.family_id, f.role, f.signal, f.noise) for f in fp.ZERO_FAMILIES] == [
        ("null", "zero", "volatility", None),
        ("vol_noise_3", "zero", "volatility", 3.0),
        ("dd_noise_3", "zero", "drawdown", 3.0),
    ]
    assert [(f.family_id, f.role, f.signal, f.noise) for f in fp.CONTROL_FAMILIES] == [
        ("vol_noise_1", "control", "volatility", 1.0),
        ("vol_noise_0.5", "control", "volatility", 0.5),
    ]
    assert list(fp.FAMILIES) == [f.family_id for f in (*fp.ZERO_FAMILIES, *fp.CONTROL_FAMILIES)]
    assert (fp.JOURNAL_FILE, fp.RESULT_JSON, fp.RESULT_MD) == (
        "false_pass_rows.jsonl",
        "false_pass_result.json",
        "false_pass_result.md",
    )


def test_seeds_are_consecutive_from_the_fixed_start() -> None:
    assert fp.seed_list(3) == [20261018, 20261019, 20261020]
    assert fp.seed_list(0) == []
    with pytest.raises(ValueError, match="negative"):
        fp.seed_list(-1)


def test_family_targets_are_the_gate_power_and_ruler_power_rules_with_another_seed() -> None:
    inputs = US.inputs
    for family_id, expected in (
        ("null", gp.planted_targets(inputs, None, seed=7, warmup=24)),
        ("vol_noise_3", gp.planted_targets(inputs, 3.0, seed=7, warmup=24)),
        ("vol_noise_1", gp.planted_targets(inputs, 1.0, seed=7, warmup=24)),
        ("vol_noise_0.5", gp.planted_targets(inputs, 0.5, seed=7, warmup=24)),
        ("dd_noise_3", rp.drawdown_targets(inputs, 3.0, seed=7, warmup=24)),
    ):
        targets = fp.family_targets(fp.FAMILIES[family_id], inputs, 7)
        pd.testing.assert_series_equal(targets, expected, check_names=True)
    # Another seed is another rule.
    one = fp.family_targets(fp.FAMILIES["null"], inputs, 7)
    other = fp.family_targets(fp.FAMILIES["null"], inputs, 8)
    assert not one.dropna().equals(other.dropna())
    with pytest.raises(ValueError, match="unknown signal"):
        fp.family_targets(fp.Family("x", "zero", "breadth", None), inputs, 7)


def test_the_warm_up_gives_a_target_on_the_gates_first_us_day() -> None:
    # The Ken French data start on 1926-07-01: 29 month ends come before stage 1's first day
    # (1929-01-02). With 24 months of warm-up the 1928-12-31 decision exists; with gate_power's 36
    # it would not, and the bench would refuse the window.
    day = pd.Timestamp("1928-12-31")
    for family in fp.ZERO_FAMILIES + fp.CONTROL_FAMILIES:
        assert math.isfinite(fp.family_targets(family, US.inputs, fp.SEED_START).loc[day])
    assert math.isnan(gp.planted_targets(US.inputs, None, seed=fp.SEED_START).loc[day])


def test_wilson_interval() -> None:
    assert fp.wilson(0, 450) == (0.0, pytest.approx(0.008464318862970662))
    assert fp.wilson(4, 450) == (
        pytest.approx(0.00346199040217373),
        pytest.approx(0.022629629458788568),
    )
    assert fp.wilson(5, 450) == (
        pytest.approx(0.004755082473599525),
        pytest.approx(0.025743362636860685),
    )
    assert fp.wilson(0, 300)[1] == pytest.approx(0.012642971224546034)
    assert fp.wilson(5, 100) == (
        pytest.approx(0.02154367915436796),
        pytest.approx(0.11175046923191913),
    )
    assert fp.wilson(3, 3) == (pytest.approx(0.4385029682449546), 1.0)
    for passes, total in ((0, 0), (-1, 5), (6, 5)):
        with pytest.raises(ValueError):
            fp.wilson(passes, total)


def test_the_stage_windows_are_the_gates_development_windows() -> None:
    windows = fp.stage_windows(LOADED, GATE)

    assert windows == {
        "us_market": Window(
            window_id="us_market_1929_2010",
            role="development",
            start=date(1929, 1, 2),
            end=date(2010, 12, 31),
        ),
        "qqq": Window(
            window_id="qqq_long_history_development",
            role="development",
            start=date(1999, 3, 10),
            end=date(2010, 12, 31),
        ),
    }


def test_the_gate_check_refuses_what_the_probe_cannot_compute() -> None:
    fp.check_gate(GATE)
    with pytest.raises(ValueError, match="confidence 0.99 is not 0.95"):
        fp.check_gate(_gate(confidence=0.99))
    with pytest.raises(ValueError, match="us_market: the probe cannot compute delay_1"):
        fp.check_gate(_gate(us_rules=[("delay_1.geometric_annual_return", ">=", -0.01)]))
    with pytest.raises(ValueError, match="qqq: the probe cannot compute leave_year_min"):
        fp.check_gate(_gate(qqq_rules=[("leave_year_min.max_drawdown", ">", 0.0)]))
    with pytest.raises(ValueError, match="cost_15"):
        fp.check_gate(_gate(us_rules=[("cost_15.geometric_annual_return", ">=", -0.01)]))


# --- one evaluation ------------------------------------------------------------------------------


@pytest.mark.parametrize(("market", "family_id"), [("us_market", "vol_noise_3"), ("qqq", "null")])
def test_an_evaluation_reads_what_a_trial_runs_falsification_gives(
    market: str, family_id: str
) -> None:
    data = DATA[market]
    window = WINDOWS[market]
    rules = GATE.markets[market].rules  # type: ignore[index]
    targets = fp.family_targets(fp.FAMILIES[family_id], data.inputs, 11)

    row = fp.evaluate(data, targets, window, LOADED, rules)

    cost = LOADED.cost_bps
    sim = simulate(
        data.returns,
        targets,
        start=window.start,
        end=window.end,
        rebalance=gp.MONTH_END,
        cost_bps=cost,
    )
    benchmark = benchmark_simulations(
        data,
        window,
        average_exposure=float(sim.exposure.mean()),
        rebalance=gp.MONTH_END,
        cost_bps=cost,
        leveraged=False,
    )[PRIMARY_BENCHMARK]
    cash = data.returns["cash"]
    comparison = compare(summarize(sim, cash, LOADED), summarize(benchmark, cash, LOADED), LOADED)
    full = falsify(
        FalsificationInputs(
            returns=data.returns,
            targets=targets,
            start=window.start,
            end=window.end,
            rebalance=gp.MONTH_END,
            delay_days=0,
            cost_bps=cost,
            strategy=sim,
            benchmark=benchmark,
        ),
        LOADED,
        0.95,
    )
    expected = evaluate_rules(rules, {**full, "comparison": comparison})

    assert row["rules"] == expected
    assert row["holds"] == all(item["holds"] for item in expected)
    assert row["error"] is None
    assert row["effect"] == {
        key: comparison[key]
        for key in ("cvar_5pct_relative_improvement", "max_drawdown", "geometric_annual_return")
    }
    assert row["average_exposure"] == pytest.approx(float(sim.exposure.mean()))
    assert row["return_days"] == len(sim.returns)
    assert (row["window_id"], row["start"], row["end"]) == (
        window.window_id,
        window.start.isoformat(),
        window.end.isoformat() if window.end else None,
    )


def test_an_evaluation_with_a_loose_rule_holds() -> None:
    rules = (
        _gate(us_rules=[("comparison.geometric_annual_return", ">=", -1.0)])
        .markets["us_market"]
        .rules
    )
    targets = fp.family_targets(fp.FAMILIES["null"], US.inputs, 3)

    row = fp.evaluate(US, targets, WINDOWS["us_market"], LOADED, rules)

    assert row["holds"] is True
    assert [item["metric"] for item in row["rules"]] == ["comparison.geometric_annual_return"]


# --- the whole run -------------------------------------------------------------------------------


def test_every_family_and_seed_is_evaluated_on_both_stages_in_order(probe: dict[str, Any]) -> None:
    expected = []
    for family in fp.ZERO_FAMILIES:
        for seed in fp.seed_list(SEEDS):
            expected += [(family.family_id, seed, "us_market"), (family.family_id, seed, "qqq")]
    for family in fp.CONTROL_FAMILIES:
        for seed in fp.seed_list(CONTROL_SEEDS):
            expected += [(family.family_id, seed, "us_market"), (family.family_id, seed, "qqq")]

    rows = probe["evaluations"]

    assert [(row["family"], row["seed"], row["market"]) for row in rows] == expected
    for row in rows:
        assert row["stage"] == {"us_market": "stage1", "qqq": "stage2"}[row["market"]]
        assert row["role"] == fp.FAMILIES[row["family"]].role
        assert row["window_id"] == WINDOWS[row["market"]].window_id
        assert row["error"] is None


def test_each_row_is_the_evaluation_of_its_family_seed_and_market(probe: dict[str, Any]) -> None:
    for index in (0, 3, 13, 25):
        row = probe["evaluations"][index]
        market = row["market"]
        data = DATA[market]
        targets = fp.family_targets(fp.FAMILIES[row["family"]], data.inputs, row["seed"])
        expected = fp.evaluate(data, targets, WINDOWS[market], LOADED, GATE.markets[market].rules)
        assert {
            key: value
            for key, value in row.items()
            if key not in ("family", "role", "seed", "market", "stage")
        } == expected


def test_the_record_names_the_gate_the_seeds_and_the_windows(probe: dict[str, Any]) -> None:
    trials = ROOT_DIR.joinpath(tr.TRIALS_DIR)

    assert probe["probe"] == "gate_false_pass"
    assert probe["run"] == "TRADING-2565-22"
    assert probe["gate_sha256"] == lg.chain_digest(fp.GATE_COPY.read_bytes())
    assert probe["prereg_sha256"] == lg.chain_digest(trials.joinpath("prereg.yaml").read_bytes())
    assert probe["plan_sha256"] == lg.chain_digest(trials.joinpath("T0001.yaml").read_bytes())
    assert probe["settings_sha256"] == LOADED.sha256
    assert probe["cutoff"] == "2010-12-31"
    assert (probe["confidence"], probe["cost_bps"], probe["warmup_months"]) == (0.95, 25.0, 24)
    assert (probe["seed_start"], probe["seeds"], probe["control_seeds"]) == (20261018, 3, 2)
    assert [f["family"] for f in probe["families"]] == list(fp.FAMILIES)
    assert probe["families"][0] == {
        "family": "null",
        "role": "zero",
        "signal": "volatility",
        "noise": None,
    }
    assert probe["windows"] == {
        "us_market": {
            "stage": "stage1",
            "window_id": "us_test",
            "start": "1929-01-02",
            "end": "1936-12-31",
        },
        "qqq": {
            "stage": "stage2",
            "window_id": "qqq_test",
            "start": "1999-03-10",
            "end": "2004-12-31",
        },
    }
    assert probe["rules"]["qqq"] == [
        {"metric": "ci_high.cvar_5pct_relative_improvement", "op": ">", "value": 0.0},
        {"metric": "comparison.geometric_annual_return", "op": ">=", "value": -0.01},
    ]
    assert len(probe["rules"]["us_market"]) == 4
    assert set(probe["provenance"]) == {"commit", "branch", "code_modified"}


def test_the_journal_holds_the_record_then_every_evaluation(
    probe_run: tuple[dict[str, Any], Path],
) -> None:
    probe, journal = probe_run
    lines = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()]

    assert len(lines) == 1 + len(probe["evaluations"])
    head = lines[0]
    assert head["tasks"] == len(probe["evaluations"])
    assert head["gate_sha256"] == probe["gate_sha256"]
    assert "evaluations" not in head and "reading" not in head
    assert lines[1:] == [gp._clean(row) for row in probe["evaluations"]]


def test_the_run_reads_the_gates_windows_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    given: list[dict[str, Window]] = []

    def fake_stage_windows(loaded: Any, gate: tr.TrialGate) -> dict[str, Window]:
        given.append({})
        return dict(WINDOWS)

    monkeypatch.setattr(fp, "stage_windows", fake_stage_windows)

    result = fp.run_probe(DATA, LOADED, GATE, ROOT_DIR, seeds=1, control_seeds=0)

    assert given == [{}]
    assert result["windows"]["us_market"]["window_id"] == "us_test"
    assert len(result["evaluations"]) == 6


def test_the_run_refuses_late_data_bad_counts_and_windows_past_the_cutoff() -> None:
    late = {"us_market": _us_data(end="2011-03-31"), "qqq": QQQ}
    with pytest.raises(ValueError, match="us_market data run past 2010-12-31"):
        fp.run_probe(late, LOADED, GATE, ROOT_DIR, seeds=1, windows=WINDOWS)
    with pytest.raises(ValueError, match="missing qqq"):
        fp.run_probe({"us_market": US}, LOADED, GATE, ROOT_DIR, seeds=1, windows=WINDOWS)
    with pytest.raises(ValueError, match="at least one seed"):
        fp.run_probe(DATA, LOADED, GATE, ROOT_DIR, seeds=0, windows=WINDOWS)
    with pytest.raises(ValueError, match="control seeds"):
        fp.run_probe(DATA, LOADED, GATE, ROOT_DIR, seeds=1, control_seeds=-1, windows=WINDOWS)
    open_end = {**WINDOWS, "qqq": replace(WINDOWS["qqq"], end=None)}
    with pytest.raises(ValueError, match="qqq: the window runs past 2010-12-31"):
        fp.run_probe(DATA, LOADED, GATE, ROOT_DIR, seeds=1, windows=open_end)
    late_window = {**WINDOWS, "us_market": replace(WINDOWS["us_market"], end=date(2011, 1, 3))}
    with pytest.raises(ValueError, match="us_market: the window runs past 2010-12-31"):
        fp.run_probe(DATA, LOADED, GATE, ROOT_DIR, seeds=1, windows=late_window)
    with pytest.raises(ValueError, match="cannot compute"):
        fp.run_probe(
            DATA,
            LOADED,
            _gate(us_rules=[("placebo_rank.max_drawdown", ">", 0.5)]),
            ROOT_DIR,
            seeds=1,
            windows=WINDOWS,
        )


def test_a_refused_evaluation_is_an_error_row_and_the_reading_is_incomplete() -> None:
    # Before 1928-12-31 no target is decided yet: the bench refuses the window.
    early = {**WINDOWS, "us_market": replace(WINDOWS["us_market"], start=date(1927, 1, 3))}

    result = fp.run_probe(DATA, LOADED, GATE, ROOT_DIR, seeds=1, control_seeds=0, windows=early)

    us_rows = [row for row in result["evaluations"] if row["market"] == "us_market"]
    assert len(us_rows) == 3
    for row in us_rows:
        assert row["error"].startswith("ValueError: no target is executable")
        assert row["holds"] is False and row["rules"] == [] and row["effect"] == {}
    assert result["summary"]["zero"]["errors"] == 3
    assert result["reading"]["verdict"] == "incomplete"


def test_a_loosened_gate_lets_the_zero_effect_rules_through(probe: dict[str, Any]) -> None:
    # The mutation check the requirement asks for: with stage 1 down to the return rule (and
    # stage 2 to a return rule nothing fails), the false-pass share rises and the reading says so.
    loose = _gate(
        us_rules=[("comparison.geometric_annual_return", ">=", -1.0)],
        qqq_rules=[("comparison.geometric_annual_return", ">=", -1.0)],
    )

    result = fp.run_probe(
        DATA, LOADED, loose, ROOT_DIR, seeds=SEEDS, control_seeds=0, windows=WINDOWS
    )

    assert result["reading"]["passes"] == 3 * SEEDS
    assert result["reading"]["verdict"] == "loose"
    assert probe["reading"]["passes"] < result["reading"]["passes"]


def test_the_probe_writes_nothing_under_the_research_root(tmp_path: Path) -> None:
    root = tmp_path / "research"
    root.joinpath(tr.TRIALS_DIR).mkdir(parents=True)
    for name in ("prereg.yaml", "T0001.yaml"):
        shutil.copyfile(ROOT_DIR / tr.TRIALS_DIR / name, root / tr.TRIALS_DIR / name)
    before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))

    fp.run_probe(DATA, LOADED, GATE, root, seeds=1, control_seeds=0, windows=WINDOWS)

    assert sorted(str(p.relative_to(root)) for p in root.rglob("*")) == before


def test_workers_give_the_same_rows() -> None:
    one = fp.run_probe(DATA, LOADED, GATE, ROOT_DIR, seeds=1, control_seeds=1, windows=WINDOWS)
    two = fp.run_probe(
        DATA, LOADED, GATE, ROOT_DIR, seeds=1, control_seeds=1, windows=WINDOWS, workers=2
    )

    assert gp._clean(two["evaluations"]) == gp._clean(one["evaluations"])


# --- summary and reading -------------------------------------------------------------------------


def _row(family: str, seed: int, stage: str, holds: bool, **extra: Any) -> dict[str, Any]:
    market = {"stage1": "us_market", "stage2": "qqq"}[stage]
    metrics = [r.metric for r in GATE.markets[market].rules]  # type: ignore[index]
    return {
        "family": family,
        "role": fp.FAMILIES[family].role,
        "seed": seed,
        "market": market,
        "stage": stage,
        "rules": [{"metric": m, "holds": holds or i == 0} for i, m in enumerate(metrics)],
        "holds": holds,
        "error": None,
        # Stage 2 effects differ, so a median over the wrong stage shows.
        "effect": {"cvar_5pct_relative_improvement": 0.01 * seed if stage == "stage1" else -1.0},
        **extra,
    }


def _families(*ids: str) -> list[dict[str, Any]]:
    return [{"family": family_id} for family_id in ids]


def test_a_seed_passes_only_when_both_its_stages_hold() -> None:
    rows = [
        _row("null", 1, "stage1", True),
        _row("null", 1, "stage2", True),
        _row("null", 2, "stage1", True),
        _row("null", 2, "stage2", False),
        _row("null", 3, "stage1", False),
        _row("null", 3, "stage2", True),
        # The same seed in another family is another rule.
        _row("vol_noise_3", 1, "stage1", False),
        _row("vol_noise_3", 1, "stage2", True),
    ]

    summary = fp.summary({"evaluations": rows, "families": _families("null", "vol_noise_3")})

    null = summary["families"]["null"]
    assert null["seeds"] == 3
    assert (null["both"]["passes"], null["both"]["total"]) == (1, 3)
    assert (null["stage1"]["passes"], null["stage1"]["total"]) == (2, 3)
    assert (null["stage2"]["passes"], null["stage2"]["total"]) == (2, 3)
    assert null["both"]["share"] == pytest.approx(1 / 3)
    assert (null["both"]["low"], null["both"]["high"]) == fp.wilson(1, 3)
    # Each rule's hold share per stage: the first rule always holds in these rows.
    first, *others = [r.metric for r in GATE.markets["us_market"].rules]
    assert null["stage1"]["rules"][first] == 1.0
    assert null["stage1"]["rules"][others[0]] == pytest.approx(2 / 3)
    assert null["stage1_effect_median"]["cvar_5pct_relative_improvement"] == pytest.approx(0.02)
    assert null["errors"] == 0
    pooled = summary["zero"]
    assert (pooled["both"]["passes"], pooled["both"]["total"]) == (1, 4)
    assert (pooled["stage1"]["passes"], pooled["stage1"]["total"]) == (2, 4)


def _reading(high: float, families: dict[str, float], errors: int = 0) -> dict[str, Any]:
    zero = {
        "both": {"passes": 1, "total": 450, "share": 1 / 450, "low": 0.0, "high": high},
        "errors": errors,
    }
    items = {
        family: {"both": {"passes": 0, "total": 150, "share": 0.0, "low": 0.0, "high": value}}
        for family, value in families.items()
    }
    return fp.reading({"summary": {"zero": zero, "families": items}})


@pytest.mark.parametrize(
    ("high", "verdict"),
    [
        (0.0, "usable"),
        (0.025, "usable"),
        (0.0250001, "reduced"),
        (0.05, "reduced"),
        (0.0500001, "loose"),
        (0.5, "loose"),
    ],
)
def test_the_reading_lines(high: float, verdict: str) -> None:
    read = _reading(high, {"null": 0.01})

    assert read["verdict"] == verdict
    assert read["upper"] == high
    assert (read["passes"], read["total"]) == (1, 450)
    assert read["share"] == pytest.approx(1 / 450)


def test_a_zero_family_above_ten_percent_is_named_and_a_control_never_is() -> None:
    read = _reading(
        0.02,
        {"null": 0.10, "vol_noise_3": 0.1000001, "dd_noise_3": 0.3, "vol_noise_1": 0.9},
    )

    assert read["flagged_families"] == ["vol_noise_3", "dd_noise_3"]
    assert read["verdict"] == "usable"


def test_an_error_or_no_seed_makes_the_reading_incomplete() -> None:
    assert _reading(0.0, {}, errors=1)["verdict"] == "incomplete"
    empty = {
        "summary": {
            "zero": {
                "both": {"passes": 0, "total": 0, "share": None, "low": None, "high": None},
                "errors": 0,
            },
            "families": {},
        }
    }
    assert fp.reading(empty)["verdict"] == "incomplete"


def test_the_run_reads_its_own_summary(probe: dict[str, Any]) -> None:
    assert probe["summary"] == fp.summary(probe)
    assert probe["reading"] == fp.reading(probe)
    pooled = probe["summary"]["zero"]["both"]
    assert pooled["total"] == 3 * SEEDS
    assert set(probe["summary"]["families"]) == set(fp.FAMILIES)


# --- output and command --------------------------------------------------------------------------


def test_the_result_is_written_as_json_and_markdown(probe: dict[str, Any], tmp_path: Path) -> None:
    fp.write_result(probe, tmp_path)

    data = json.loads(tmp_path.joinpath(fp.RESULT_JSON).read_text(encoding="utf-8"))
    assert data["reading"] == gp._clean(probe["reading"])
    assert len(data["evaluations"]) == len(probe["evaluations"])
    text = tmp_path.joinpath(fp.RESULT_MD).read_text(encoding="utf-8")
    assert text.startswith("# 误放率探针（TRADING-2565-22）")
    assert f"判读：{probe['reading']['verdict']}" in text
    for family in fp.FAMILIES:
        assert f"| {family} |" in text
    assert "trial_gate_v2.yaml" in text
    assert "只量门，不进评估台账、筛选账和试跑账，不算试验，不改试跑门。" in text


def test_the_command_runs_the_probe_and_writes_the_result(
    probe: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        kwargs["journal"].write_text("{}\n", encoding="utf-8")
        return dict(probe)

    monkeypatch.setattr(fp, "load_datasets", lambda root: (DATA, GATE, LOADED, ROOT_DIR))
    monkeypatch.setattr(fp, "run_probe", fake_run)
    out = tmp_path / "false_pass"

    result = CliRunner().invoke(
        fp.app,
        ["run", "--out", str(out), "--workers", "3", "--seeds", "5", "--control-seeds", "0"],
    )

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert (calls[0]["workers"], calls[0]["seeds"], calls[0]["control_seeds"]) == (3, 5, 0)
    assert calls[0]["journal"] == out / fp.JOURNAL_FILE
    assert "判读" in result.output and "95% 上限" in result.output
    data = json.loads(out.joinpath(fp.RESULT_JSON).read_text(encoding="utf-8"))
    assert data["source"] == {"us_market": US.source, "qqq": QQQ.source}
    assert out.joinpath(fp.RESULT_MD).exists()

    # A directory that already holds a run is refused: a finished result is never overwritten.
    again = CliRunner().invoke(fp.app, ["run", "--out", str(out)])
    assert again.exit_code == 1
    assert "已有结果" in again.output
    assert len(calls) == 1


def test_the_command_defaults_are_the_fixed_counts(
    probe: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return dict(probe)

    monkeypatch.setattr(fp, "load_datasets", lambda root: (DATA, GATE, LOADED, ROOT_DIR))
    monkeypatch.setattr(fp, "run_probe", fake_run)

    result = CliRunner().invoke(fp.app, ["run", "--out", str(tmp_path / "x")])

    assert result.exit_code == 0, result.output
    assert (calls[0]["workers"], calls[0]["seeds"], calls[0]["control_seeds"]) == (1, 150, 20)


def test_the_command_stops_when_the_data_do_not_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(root: Path) -> Any:
        raise ValueError("snapshot not checked")

    monkeypatch.setattr(fp, "load_datasets", refuse)

    result = CliRunner().invoke(fp.app, ["run", "--out", str(tmp_path / "x")])

    assert result.exit_code == 1
    assert "数据载入失败：snapshot not checked" in result.output
    assert not tmp_path.joinpath("x").exists()


def test_the_loaded_data_are_cut_at_2010_and_come_with_the_gate_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    late = {"us_market": _us_data(end="2011-06-30"), "qqq": _qqq_data()}
    late["qqq"] = BenchData(
        returns=late["qqq"].returns.reindex(pd.bdate_range("1994-01-03", "2011-06-30")),
        inputs=late["qqq"].inputs.reindex(pd.bdate_range("1994-01-03", "2011-06-30")),
        source=late["qqq"].source,
    )
    given: list[tuple[str, tr.TrialGate, str]] = []

    def fake_data(market: str, gate: tr.TrialGate, plan: Any, **kwargs: Any) -> BenchData:
        given.append((market, gate, plan.trial_id))
        return late[market]

    monkeypatch.setattr(tr, "market_data", fake_data)

    datasets, gate, loaded, root_dir = fp.load_datasets(Path("unused"))

    assert root_dir == ROOT_DIR
    assert gate == fp.load_gate() == tr.load_gate(fp.GATE_COPY)
    assert given == [("us_market", gate, "T0001"), ("qqq", gate, "T0001")]
    for market, data in datasets.items():
        assert pd.Timestamp(data.returns.index.max()) == pd.Timestamp("2010-12-31"), market
        assert pd.Timestamp(data.inputs.index.max()) == pd.Timestamp("2010-12-31"), market
