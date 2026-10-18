"""Trial mode of the research loop (TRADING-2565 S5b): the gate, the plans, the trial runs and
their chain, the verdict, the summary and report, the commands and the research records lane.

Written before the code. The bench and the signal are real (``tsmom_21``, which needs no outside
source); the data are synthetic and the repositories temporary.
"""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import BenchCheckError, RunSpec, load_settings, run_strategy
from ai_trading_system.research_bench import __main__ as cli
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import (
    BenchData,
    from_daily_frame,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_bench.prereg import Rule, evaluate_rules
from ai_trading_system.research_bench.run import spec_record
from ai_trading_system.research_bench.screen import load_signal
from ai_trading_system.research_bench.screen_ledger import append_screen_record
from ai_trading_system.research_bench.signal_strategy import signal_exposure
from ai_trading_system.research_bench.simulate import Rebalance
from ai_trading_system.research_bench.sources import SourceSpec
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_data.signal_sources import SignalSnapshot
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import ruler_power as rp
from tools.gov008 import ship

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
DECISION = "owner_decision:TRADING-2565:2026-10-19:tail_floor10"
GATE_FILE = PROJECT_ROOT / "research/_trials/prereg.yaml"
PLAN_FILE = PROJECT_ROOT / "research/_trials/T0001.yaml"
SIG = "sig-0123456789ab"
QLH = "qlh-test-snapshot"
TSMOM = load_signal("tsmom_21")
REF = {
    "id": "tsmom_21",
    "module_sha256": TSMOM.module_sha256,
    "spec_sha256": TSMOM.spec_sha256,
    "score": "sign",
}
CRASH_IDS = [
    "crash_1929",
    "flash_crash_1962",
    "crash_1987",
    "ltcm_1998",
    "dotcom_bear",
    "global_financial_crisis",
]


def _yaml(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


GATE = _yaml(GATE_FILE)


def _config(config_id: str, low: float = 0.0, high: float = 1.0, **changes: Any) -> dict[str, Any]:
    params = {"signals": [dict(REF)], "mapping": "threshold", "low": low, "high": high}
    return {
        "id": config_id,
        "function": "signal_exposure",
        "params": params,
        "rebalance": {"schedule": "month_end", "band": 0.0},
        **changes,
    }


def _plan(trial_id: str = "T0001", **changes: Any) -> dict[str, Any]:
    return {
        "schema_version": "research_trial_plan.v1",
        "trial_id": trial_id,
        "candidate": {"signals": ["tsmom_21"], "screens": ["S0001"]},
        "hypothesis_card": "test card",
        "from_validation_window": False,
        "data": {"snapshot_id": QLH, "signal_snapshot_id": SIG},
        "primary": "primary",
        "configs": [_config("primary"), _config("half", low=0.5)],
        "production_effect": "none",
        "broker_action": "none",
        **changes,
    }


def _git(cwd: Path, *args: str) -> str:
    done = subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)
    return done.stdout.strip()


def _screen(
    top: Path, signal_id: str = "tsmom_21", family_size: int = 3, signal_snapshot: str = SIG
) -> None:
    append_screen_record(
        top,
        {
            "signal_id": signal_id,
            "module_sha256": "a" * 64,
            "spec_sha256": "b" * 64,
            "params": {},
            "markets": ["qqq", "us_market"],
            "data": {"signal_snapshot_id": signal_snapshot, "bench_snapshot_id": QLH},
            "code": {"commit": "c" * 40, "modified": False},
            "settings_sha256": "d" * 64,
            "results": {},
            "primary_p_value": 0.01,
            "verdict": "candidate",
            "family_size": family_size,
        },
    )


def _write(path: Path, data: dict[str, Any]) -> None:
    path.write_bytes(yaml.safe_dump(data, sort_keys=False).encode("utf-8"))


def _repo(
    base: Path,
    *,
    plan: dict[str, Any] | None = None,
    approve: bool = True,
    screen: str | None = "tsmom_21",
    commit_plan: bool = True,
) -> Path:
    """A repository with the real gate, one plan, a screen record and the decision on main;
    returns its research root."""

    top = base.joinpath("repo")
    trials = top.joinpath("research", "_trials")
    trials.mkdir(parents=True)
    top.joinpath("docs", "requirements").mkdir(parents=True)
    trials.joinpath("prereg.yaml").write_bytes(GATE_FILE.read_bytes())
    _git(top, "init", "-q", "-b", "main")
    _git(top, "config", "user.email", "test@example.com")
    _git(top, "config", "user.name", "test")
    if screen is not None:
        _screen(top, screen)
    if approve:
        top.joinpath("docs", "requirements", "TRADING-2565.md").write_text(
            f"- {DECISION}: trial mode approved.\n", encoding="utf-8"
        )
    if commit_plan:
        _write(trials.joinpath("T0001.yaml"), plan or _plan())
    _git(top, "add", "-A")
    _git(top, "commit", "-q", "-m", "trial gate and plan")
    if not commit_plan:
        _write(trials.joinpath("T0001.yaml"), plan or _plan())
    return top.joinpath("research")


def _qqq_data(
    snapshot: str = QLH, series: str = "actual", signal: str | None = SIG, seed: int = 3
) -> BenchData:
    index = pd.bdate_range("1997-01-02", "2011-06-30", name="date")
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
    data = from_daily_frame(
        daily,
        qqq_series="actual" if series == "actual" else "proxy",
        source={"snapshot_id": snapshot},
    )
    return data if signal is None else with_signal_sources(data, {}, {}, snapshot_id=signal)


def _factors(start: str = "1926-07-01", end: str = "2011-06-30", seed: int = 5) -> pd.DataFrame:
    index = pd.bdate_range(start, end, name="date")
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "Mkt-RF": rng.normal(0.0003, 0.011, len(index)),
            "SMB": 0.0,
            "HML": 0.0,
            "RF": 0.0001,
        },
        index=index,
    )


def _us_data(signal: str = SIG) -> BenchData:
    data = from_us_market({"ff_factors_daily": _factors()}, snapshot_id=signal)
    return with_signal_sources(data, {}, {}, snapshot_id=signal)


QQQ = _qqq_data()
US = _us_data()


def _runs(root: Path) -> list[Path]:
    runs = root.joinpath("_trials", "runs")
    return sorted(runs.glob("*.json")) if runs.exists() else []


def _sha(path: Path) -> str:
    return lg.chain_digest(path.read_bytes())


# The gate and the first plan ----------------------------------------------------------------------


def test_the_gate_is_the_two_stage_ruler_the_probe_measured() -> None:
    # Stage 1 on the US market's 82 years is the core four rules as the ruler probe applied them
    # (the measured gate's own rules on those metrics) and the left-tail floor of
    # owner_decision:TRADING-2565:2026-10-19:tail_floor10; the QQQ development window is
    # stage 2, S3.
    gate = tr.load_gate(GATE_FILE)
    measured = {
        r.metric: (r.metric, r.op, r.value) for r in gp.load_measured_gate().markets["qqq"].rules
    }
    floor = ("comparison.cvar_5pct_relative_improvement", ">=", 0.10)

    assert gate.owner_decision == DECISION
    assert (gate.max_configs_per_trial, gate.confidence) == (8, 0.95) == (8, gp.CONFIDENCE)
    qqq, us = gate.markets["qqq"], gate.markets["us_market"]
    assert (qqq.question_type, qqq.qqq_series, qqq.event_episodes) == (
        "qqq_direction",
        "actual",
        None,
    )
    assert [(r.metric, r.op, r.value) for r in qqq.rules] == [
        ("ci_high.cvar_5pct_relative_improvement", ">", 0.0),
        measured[rp.GEO_METRIC],
    ]
    assert [r.metric for r in us.rules] == [*rp.CORE_METRICS, floor[0]]
    assert [(r.metric, r.op, r.value) for r in us.rules] == [
        *(measured[m] for m in rp.CORE_METRICS),
        floor,
    ]
    assert (us.question_type, us.qqq_series) == ("us_market_trial", None)
    assert us.event_episodes is not None
    assert [(e.id, e.start, e.end) for e in us.event_episodes] == [
        ("crash_1929", "1929-10", "1929-11"),
        ("flash_crash_1962", "1962-05", "1962-06"),
        ("crash_1987", "1987-10", "1987-10"),
        ("ltcm_1998", "1998-08", "1998-10"),
        ("dotcom_bear", "2000-03", "2002-10"),
        ("global_financial_crisis", "2007-10", "2009-03"),
    ]
    assert [episode.id for episode in us.event_episodes] == CRASH_IDS
    assert tr.gate_problems(gate, FULL) == []


def test_the_first_plan_tries_industry_corr_63() -> None:
    gate = tr.load_gate(GATE_FILE)
    plan = tr.load_plan(PLAN_FILE, gate)
    corr = load_signal("industry_corr_63")

    assert plan.trial_id == "T0001"
    assert plan.candidate.signals == ("industry_corr_63",)
    assert plan.candidate.screens == ("S0009",)
    assert plan.from_validation_window is False
    assert (plan.data.snapshot_id, plan.data.signal_snapshot_id) == (
        "qlh-20261013-0b0df53cb1a2",
        "sig-7ad8aa00b736",
    )
    assert [config.id for config in plan.configs] == [
        "primary",
        "threshold",
        "floor_half",
        "weekly",
    ]
    assert plan.primary == "primary"
    primary = plan.config("primary")
    assert primary.params == {
        "signals": [
            {
                "id": "industry_corr_63",
                "module_sha256": corr.module_sha256,
                "spec_sha256": corr.spec_sha256,
                "score": "percentile",
            }
        ],
        "mapping": "linear",
        "low": 0,
        "high": 1,
    }
    assert primary.rebalance.schedule == "month_end"
    assert plan.config("weekly").rebalance.schedule == "week_end"
    assert tr.plan_problems(PROJECT_ROOT / "research", plan) == []


# Formats -----------------------------------------------------------------------------------------


def _bad_plans() -> list[tuple[str, dict[str, Any], str]]:
    other = dict(REF, id="tsmom_63")
    steps = _config("primary")
    steps["params"] = {
        "signals": [dict(REF)],
        "mapping": "steps",
        "edges": [0.5],
        "levels": [0.0, 1.2],
    }
    no_mapping = _config("primary")
    del no_mapping["params"]["mapping"]
    exact = "signals must be exactly the candidate's"
    between = "exposures lie between 0 and 1"
    pattern = "should match pattern"
    return [
        ("extra field", _plan(note="x"), "Extra inputs are not permitted"),
        (
            "other function",
            _plan(configs=[_config("primary", function="trend")]),
            "function must be signal_exposure",
        ),
        ("delay", _plan(configs=[_config("primary", delay_days=1)]), "no delay"),
        ("leverage", _plan(configs=[_config("primary", leveraged_benchmarks=True)]), "no delay"),
        ("primary missing", _plan(primary="nope"), "primary nope is not a config"),
        (
            "duplicate ids",
            _plan(configs=[_config("primary"), _config("primary")]),
            "config ids must be unique",
        ),
        ("high above 1", _plan(configs=[_config("primary", high=1.5)]), between),
        ("low below 0", _plan(configs=[_config("primary", low=-0.1)]), between),
        ("steps level above 1", _plan(configs=[steps]), between),
        ("bool level", _plan(configs=[_config("primary", low=True)]), "must be numbers"),
        ("no mapping", _plan(configs=[no_mapping]), "mapping must be linear"),
        (
            "other signal",
            _plan(
                configs=[
                    {**_config("primary"), "params": {**_config("x")["params"], "signals": [other]}}
                ]
            ),
            exact,
        ),
        (
            "extra signal",
            _plan(
                configs=[
                    {
                        **_config("primary"),
                        "params": {**_config("x")["params"], "signals": [dict(REF), other]},
                    }
                ]
            ),
            exact,
        ),
        (
            "missing signal",
            _plan(candidate={"signals": ["tsmom_21", "tsmom_63"], "screens": ["S0001"]}),
            exact,
        ),
        (
            "duplicate candidate",
            _plan(candidate={"signals": ["tsmom_21", "tsmom_21"], "screens": ["S0001"]}),
            "candidate signals must be unique",
        ),
        ("no screens", _plan(candidate={"signals": ["tsmom_21"], "screens": []}), "at least 1"),
        ("bad screen id", _plan(candidate={"signals": ["tsmom_21"], "screens": ["X1"]}), pattern),
        ("flag not a bool", _plan(from_validation_window="no"), "valid boolean"),
        (
            "bad signal snapshot",
            _plan(data={"snapshot_id": QLH, "signal_snapshot_id": "sig-xyz"}),
            pattern,
        ),
        (
            "latest snapshot",
            _plan(data={"snapshot_id": "LATEST", "signal_snapshot_id": SIG}),
            "not LATEST",
        ),
        ("no data", {k: v for k, v in _plan().items() if k != "data"}, "Field required"),
        ("bad trial id", _plan(trial_id="T1"), pattern),
        ("production effect", _plan(production_effect="paper"), "'none'"),
    ]


@pytest.mark.parametrize(
    ("name", "plan", "match"), _bad_plans(), ids=[name for name, _, _ in _bad_plans()]
)
def test_plan_format_refusals(tmp_path: Path, name: str, plan: dict[str, Any], match: str) -> None:
    path = tmp_path.joinpath("T0001.yaml")
    _write(path, plan)
    with pytest.raises(ValueError, match=match):
        tr.load_plan(path)


def test_a_valid_plan_loads_and_its_file_name_must_match(tmp_path: Path) -> None:
    path = tmp_path.joinpath("T0001.yaml")
    _write(path, _plan())
    plan = tr.load_plan(path)
    assert [config.id for config in plan.configs] == ["primary", "half"]
    assert plan.config("half").params["low"] == 0.5

    renamed = tmp_path.joinpath("T0002.yaml")
    _write(renamed, _plan())
    with pytest.raises(ValueError, match="T0001"):
        tr.load_plan(renamed)


def test_more_configs_than_the_gate_allows_are_refused(tmp_path: Path) -> None:
    gate = tr.TrialGate.model_validate({**GATE, "max_configs_per_trial": 2})
    path = tmp_path.joinpath("T0001.yaml")
    _write(path, _plan(configs=[_config("primary"), _config("a"), _config("b")]))

    assert len(tr.load_plan(path).configs) == 3
    with pytest.raises(ValueError, match="allows 2"):
        tr.load_plan(path, gate)
    _write(path, _plan(configs=[_config("primary"), _config("a")]))
    assert len(tr.load_plan(path, gate).configs) == 2


def _gate_with(**changes: Any) -> dict[str, Any]:
    gate = copy.deepcopy(GATE)
    for dotted, value in changes.items():
        keys = dotted.split("__")
        target = gate
        for key in keys[:-1]:
            target = target[key]
        if value is None:
            del target[keys[-1]]
        else:
            target[keys[-1]] = value
    return gate


@pytest.mark.parametrize(
    "gate",
    [
        _gate_with(markets__us_market=None),
        _gate_with(markets__nasdaq=copy.deepcopy(GATE["markets"]["us_market"])),
        _gate_with(markets__qqq__qqq_series=None),
        _gate_with(markets__us_market__qqq_series="actual"),
        _gate_with(
            markets__us_market__event_episodes=[{"id": "x", "start": "1929-11", "end": "1929-10"}]
        ),
        _gate_with(markets__us_market__rules=[]),
        _gate_with(note="x"),
        _gate_with(confidence=1.0),
        _gate_with(max_configs_per_trial=0),
        _gate_with(owner_decision="trial mode"),
        _gate_with(production_effect=None),
    ],
)
def test_gate_format_refusals(gate: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        tr.TrialGate.model_validate(gate)


def test_gate_problems_name_unknown_metrics_and_windows() -> None:
    rules = [
        *GATE["markets"]["qqq"]["rules"],
        {"metric": "comparison.no_such_field", "op": ">", "value": 0},
    ]
    gate = tr.TrialGate.model_validate(
        _gate_with(markets__qqq__rules=rules, markets__us_market__question_type="no_such_type")
    )
    problems = tr.gate_problems(gate, FULL)

    assert "qqq: no metric comparison.no_such_field" in problems
    assert any(
        problem.startswith("us_market: ") and "no_such_type" in problem for problem in problems
    )
    assert len(problems) == 2


# Plans of the same signal, pinned signals ---------------------------------------------------------


def test_a_second_plan_for_the_same_signal_is_refused(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    _write(root.joinpath("_trials", "T0002.yaml"), _plan("T0002"))
    plan = tr.load_plan(root.joinpath("_trials", "T0001.yaml"))

    assert tr.plan_problems(root, plan) == ["T0002 already tries tsmom_21"]


def test_a_changed_signal_pin_is_refused(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    stale = dict(REF, module_sha256="0" * 64)
    config = {**_config("primary"), "params": {**_config("x")["params"], "signals": [stale]}}
    path = root.joinpath("_trials", "T0001.yaml")
    _write(path, _plan(configs=[config]))

    assert tr.plan_problems(root, tr.load_plan(path)) == [
        "primary: signal tsmom_21 changed since the plan"
    ]


# Approval, data and arguments: refused before anything runs, nothing recorded ---------------------


def _refused(
    root: Path, match: str, config: str = "primary", market: str = "qqq", data: BenchData = QQQ
) -> None:
    with pytest.raises(tr.TrialError, match=match):
        tr.run_trial(root, "T0001", config, market, data, loaded=LOADED)
    assert _runs(root) == []


def test_an_unpublished_plan_is_refused(tmp_path: Path) -> None:
    _refused(_repo(tmp_path, commit_plan=False), "research/_trials/T0001.yaml is not on main")


def test_a_plan_that_differs_from_main_is_refused(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    _write(root.joinpath("_trials", "T0001.yaml"), _plan(hypothesis_card="changed"))
    _refused(root, "research/_trials/T0001.yaml differs from the version on main")


def test_an_unpublished_gate_is_refused(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    gate = root.joinpath("_trials", "prereg.yaml")
    gate.write_text(gate.read_text(encoding="utf-8") + "# edited\n", encoding="utf-8")
    _refused(root, "research/_trials/prereg.yaml differs from the version on main")


def test_the_gate_decision_must_be_in_main_requirements(tmp_path: Path) -> None:
    _refused(_repo(tmp_path, approve=False), f"{DECISION} is not in docs/requirements on main")


def test_the_plan_screens_must_be_in_the_screening_ledger(tmp_path: Path) -> None:
    _refused(_repo(tmp_path, screen=None), "screen S0001 is not in the screening ledger")


def test_the_plan_screen_must_have_screened_its_signal(tmp_path: Path) -> None:
    _refused(
        _repo(tmp_path, screen="tsmom_63"), "screen S0001 screened tsmom_63, not a signal of T0001"
    )


def test_a_second_plan_of_the_signal_refuses_the_run(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    _write(root.joinpath("_trials", "T0002.yaml"), _plan("T0002"))
    _refused(root, "T0002 already tries tsmom_21")


@pytest.mark.parametrize(
    ("market", "data"),
    [
        ("qqq", _qqq_data(snapshot="qlh-other")),
        ("qqq", _qqq_data(series="proxy")),
        ("qqq", _qqq_data(signal="sig-ffffffffffff")),
        ("qqq", _qqq_data(signal=None)),
        ("qqq", US),
        ("us_market", QQQ),
        ("us_market", _us_data(signal="sig-ffffffffffff")),
    ],
)
def test_data_must_be_the_plan_snapshots(tmp_path: Path, market: str, data: BenchData) -> None:
    _refused(_repo(tmp_path), "T0001 data", market=market, data=data)


def test_unknown_configs_markets_and_trial_ids_are_refused(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    _refused(root, "T0001 has no config nope", config="nope")
    _refused(root, "the gate has no market nasdaq", market="nasdaq")
    for trial_id in ("T1", "../_trials/T0001", "t0001"):
        with pytest.raises(tr.TrialError, match="is not a trial id"):
            tr.run_trial(root, trial_id, "primary", "qqq", QQQ, loaded=LOADED)
    with pytest.raises(tr.TrialError, match="T0009.yaml"):
        tr.run_trial(root, "T0009", "primary", "qqq", QQQ, loaded=LOADED)
    assert _runs(root) == []


# Successful runs ---------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def trial(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = _repo(tmp_path_factory.mktemp("trial"))
    records = [
        tr.run_trial(root, "T0001", "primary", "qqq", QQQ, loaded=LOADED),
        tr.run_trial(root, "T0001", "primary", "us_market", US, loaded=LOADED),
        tr.run_trial(root, "T0001", "half", "qqq", QQQ, loaded=LOADED),
    ]
    return {"root": root, "records": records}


def _spec(config: str, market: str) -> RunSpec:
    low = 0.5 if config == "half" else 0.0
    return RunSpec(
        question_type="qqq_direction" if market == "qqq" else "us_market_trial",
        strategy_id=config,
        params={"signals": [dict(REF)], "mapping": "threshold", "low": low, "high": 1.0},
        window_role="development",
        rebalance=Rebalance(schedule="month_end"),
    )


def test_a_trial_run_record_carries_the_plan_the_gate_and_the_result(trial: dict[str, Any]) -> None:
    root = trial["root"]
    record = trial["records"][0]
    trials = root.joinpath("_trials")

    assert record["schema_version"] == "research_trial_run.v1"
    assert (record["seq"], record["run_id"], record["prev_sha256"]) == (1, "trial-r0001", None)
    assert (record["trial_id"], record["config"], record["market"]) == ("T0001", "primary", "qqq")
    assert record["strategy"] == {
        "id": "primary",
        "function": "signal_exposure",
        "module": "T0001.yaml",
        "module_sha256": _sha(trials.joinpath("T0001.yaml")),
    }
    assert record["prereg_sha256"] == _sha(trials.joinpath("prereg.yaml"))
    assert record["spec"] == spec_record(_spec("primary", "qqq"))
    assert record["cost_bps"] == LOADED.cost_bps
    assert record["falsification_confidence"] == 0.95
    assert record["window"] == {
        "window_id": "qqq_long_history_development",
        "role": "development",
        "start": "1999-03-10",
        "end": "2010-12-31",
    }
    assert record["source"] == QQQ.source
    assert record["settings_sha256"] == LOADED.sha256
    assert record["provenance"]["commit"] == _git(root.parent, "rev-parse", "HEAD")
    assert record["provenance"]["code_modified"] is False
    assert record["status"] == "ok"
    assert set(record["result"]) == {
        "strategy",
        "benchmarks",
        "comparison",
        "exposure_gap",
        "checks",
        "fingerprint",
        "falsification",
        "events",
        "diagnostics",
    }
    assert isinstance(record["recorded_at"], str) and record["recorded_at"].endswith("Z")


def test_the_record_matches_a_direct_bench_run(trial: dict[str, Any]) -> None:
    for record, config, market, data in (
        (trial["records"][0], "primary", "qqq", QQQ),
        (trial["records"][2], "half", "qqq", QQQ),
    ):
        direct = run_strategy(
            data, signal_exposure, _spec(config, market), LOADED, falsification_confidence=0.95
        )
        assert record["result"]["fingerprint"] == direct.fingerprint
        assert record["result"]["comparison"] == pytest.approx(direct.comparison, nan_ok=True)


def test_gate_rows_are_the_gate_rules_on_the_result(trial: dict[str, Any]) -> None:
    gate = tr.load_gate(GATE_FILE)
    for record in trial["records"]:
        rules: tuple[Rule, ...] = gate.markets[record["market"]].rules
        result = record["result"]
        expected = lg._plain(evaluate_rules(rules, {**result["falsification"], **result}))
        assert record["gate"] == expected
        assert [row["metric"] for row in expected] == [rule.metric for rule in rules]
        assert record["gate_holds"] is all(row["holds"] for row in expected)


def test_the_us_market_run_uses_its_window_and_crash_episodes(trial: dict[str, Any]) -> None:
    record = trial["records"][1]

    assert record["market"] == "us_market"
    assert record["window"] == {
        "window_id": "us_market_1929_2010",
        "role": "development",
        "start": "1929-01-02",
        "end": "2010-12-31",
    }
    assert record["spec"] == spec_record(_spec("primary", "us_market"))
    assert record["source"] == US.source
    assert list(record["result"]["events"]["episodes"]) == CRASH_IDS
    assert record["falsification_confidence"] == 0.95
    assert [row["metric"] for row in record["gate"]] == [
        *rp.CORE_METRICS,
        "comparison.cvar_5pct_relative_improvement",
    ]


def test_records_are_chained_and_written_once(trial: dict[str, Any]) -> None:
    root = trial["root"]
    paths = _runs(root)

    assert [path.name for path in paths] == ["0001.json", "0002.json", "0003.json"]
    assert tr.verify_trial_runs(root) == []
    previous = None
    for path, record in zip(paths, tr.trial_records(root), strict=True):
        text = path.read_text(encoding="utf-8")
        assert record["prev_sha256"] == previous
        assert text == json.dumps(record, ensure_ascii=False, indent=1, sort_keys=True) + "\n"
        previous = lg.chain_digest(path.read_bytes())
    assert tr.trial_records(root) == trial["records"]


def test_a_successful_run_is_not_repeated(trial: dict[str, Any]) -> None:
    root = trial["root"]
    with pytest.raises(tr.TrialError, match="trial-r0001 already ran primary on qqq"):
        tr.run_trial(root, "T0001", "primary", "qqq", QQQ, loaded=LOADED)
    assert len(_runs(root)) == 3


# Failed runs, changed plans, broken chains -------------------------------------------------------


def test_failed_runs_are_recorded_and_may_run_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("boom")

    def check(*args: Any, **kwargs: Any) -> Any:
        raise BenchCheckError(["look-ahead on 2001-01-02"])

    monkeypatch.setattr(tr, "run_strategy", boom)
    first = tr.run_trial(root, "T0001", "primary", "qqq", QQQ, loaded=LOADED)
    monkeypatch.setattr(tr, "run_strategy", check)
    second = tr.run_trial(root, "T0001", "primary", "qqq", QQQ, loaded=LOADED)
    monkeypatch.undo()
    third = tr.run_trial(root, "T0001", "primary", "qqq", QQQ, loaded=LOADED)

    assert (first["status"], first["error"]) == ("error", "RuntimeError: boom")
    assert (second["status"], second["problems"]) == ("check_failed", ["look-ahead on 2001-01-02"])
    for failed in (first, second):
        assert not {"result", "gate", "gate_holds"} & set(failed)
        assert failed["strategy"]["module_sha256"] == third["strategy"]["module_sha256"]
    assert (third["status"], third["seq"]) == ("ok", 3)
    assert [record["status"] for record in tr.trial_records(root)] == [
        "error",
        "check_failed",
        "ok",
    ]


def _error_run(
    root: Path, monkeypatch: pytest.MonkeyPatch, config: str = "primary"
) -> dict[str, Any]:
    def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(tr, "run_strategy", boom)
    try:
        return tr.run_trial(root, "T0001", config, "qqq", QQQ, loaded=LOADED)
    finally:
        monkeypatch.undo()


def test_a_plan_changed_after_a_run_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    _error_run(root, monkeypatch)
    _write(root.joinpath("_trials", "T0001.yaml"), _plan(hypothesis_card="a later idea"))
    _git(root.parent, "commit", "-q", "-am", "change the plan")

    with pytest.raises(tr.TrialError, match="T0001 changed after its run trial-r0001"):
        tr.run_trial(root, "T0001", "half", "qqq", QQQ, loaded=LOADED)
    assert len(_runs(root)) == 1


def test_a_broken_chain_is_found_and_refuses_new_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    _error_run(root, monkeypatch)
    _error_run(root, monkeypatch, "half")
    first = root.joinpath("_trials", "runs", "0001.json")
    original = first.read_bytes()

    assert b"boom" in original
    first.write_bytes(original.replace(b"boom", b"BOOM"))
    assert tr.verify_trial_runs(root) == [
        "runs/0002.json: prev_sha256 does not match the record before it"
    ]
    with pytest.raises(tr.TrialError, match="broken"):
        tr.run_trial(root, "T0001", "primary", "qqq", QQQ, loaded=LOADED)
    with pytest.raises(tr.TrialError, match="broken"):
        tr.trial_records(root)

    first.write_bytes(original.replace(b'"trial-r0001"', b'"trial-r0007"'))
    assert tr.verify_trial_runs(root) == [
        "runs/0001.json: run_id trial-r0007 does not match the file name",
        "runs/0002.json: prev_sha256 does not match the record before it",
    ]
    first.unlink()
    assert "runs/0002.json: expected record 0001; a record is missing" in tr.verify_trial_runs(root)
    first.write_bytes(original)
    root.joinpath("_trials", "runs", "notes.txt").write_text("x", encoding="utf-8")
    assert tr.verify_trial_runs(root) == ["runs/notes.txt is not a trial run record"]
    assert len(_runs(root)) == 2


def test_the_chain_uses_the_line_ending_neutral_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    _error_run(root, monkeypatch)
    first = root.joinpath("_trials", "runs", "0001.json")
    first.write_bytes(first.read_bytes().replace(b"\n", b"\r\n"))

    assert tr.verify_trial_runs(root) == []
    second = _error_run(root, monkeypatch, "half")
    assert second["prev_sha256"] == lg.chain_digest(first.read_bytes())
    assert second["prev_sha256"] != hashlib.sha256(first.read_bytes()).hexdigest()


def test_a_record_file_is_never_overwritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    _error_run(root, monkeypatch)
    first = root.joinpath("_trials", "runs", "0001.json")
    original = first.read_bytes()

    # Another writer's record appears between reading the chain and writing the file.
    monkeypatch.setattr(tr, "trial_records", lambda root_dir: [])
    monkeypatch.setattr(tr, "_record_paths", lambda root_dir: [])
    with pytest.raises(FileExistsError):
        _error_run(root, monkeypatch, "half")
    assert first.read_bytes() == original


# The verdict -------------------------------------------------------------------------------------


def _record(
    market: str, holds: bool | None, config: str = "primary", trial: str = "T0001"
) -> dict[str, Any]:
    status = "ok" if holds is not None else "error"
    record: dict[str, Any] = {
        "trial_id": trial,
        "config": config,
        "market": market,
        "status": status,
    }
    if holds is not None:
        record["gate_holds"] = holds
    return record


@pytest.mark.parametrize(
    ("records", "verdict"),
    [
        ([], "incomplete"),
        ([_record("qqq", True)], "incomplete"),
        ([_record("qqq", True), _record("us_market", True)], "pass"),
        ([_record("qqq", False)], "fail"),
        ([_record("qqq", True), _record("us_market", False)], "fail"),
        ([_record("qqq", None), _record("us_market", True)], "incomplete"),
        ([_record("qqq", None), _record("qqq", True), _record("us_market", True)], "pass"),
        (
            [
                _record("qqq", False, config="half"),
                _record("qqq", True),
                _record("us_market", True),
            ],
            "pass",
        ),
        ([_record("qqq", False, trial="T0002"), _record("qqq", True)], "incomplete"),
        ([_record("qqq", False), _record("qqq", True), _record("us_market", True)], "fail"),
    ],
)
def test_the_primary_configuration_decides(records: list[dict[str, Any]], verdict: str) -> None:
    assert tr.primary_verdict(records, "T0001", "primary") == verdict


# Ledger, summary and report ----------------------------------------------------------------------


def _expected_verdict(records: list[dict[str, Any]]) -> str:
    holds = [r["gate_holds"] for r in records if r["config"] == "primary"]
    return "fail" if not all(holds) else "pass"


def test_the_ledger_counts_configs_and_question_trials(
    trial: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = trial["root"]
    summary = tr.trial_ledger_summary(root)

    assert summary["problems"] == []
    assert (summary["trial_configs"], summary["question_trials"], summary["global_trials"]) == (
        2,
        0,
        2,
    )
    item = summary["trials"]["T0001"]
    assert (item["configs"], item["max_configs"], item["primary"]) == (2, 8, "primary")
    assert item["verdict"] == _expected_verdict(trial["records"])
    assert [
        (run["run_id"], run["config"], run["market"], run["status"]) for run in item["runs"]
    ] == [
        ("trial-r0001", "primary", "qqq", "ok"),
        ("trial-r0002", "primary", "us_market", "ok"),
        ("trial-r0003", "half", "qqq", "ok"),
    ]
    first = trial["records"][0]
    assert item["runs"][0]["rules_held"] == sum(row["holds"] for row in first["gate"])
    assert item["runs"][0]["rules"] == 2
    assert item["runs"][0]["failed"] == [row["metric"] for row in first["gate"] if not row["holds"]]

    monkeypatch.setattr(lg, "ledger_summary", lambda root_dir: {"global_trials": 7})
    counted = tr.trial_ledger_summary(root)
    assert (counted["question_trials"], counted["global_trials"]) == (7, 9)


def _reads(directory: Path, entries: list[tuple[str, str]]) -> None:
    directory.mkdir(parents=True)
    directory.joinpath("prereg.yaml").write_text("{}\n", encoding="utf-8")
    lines: list[bytes] = []
    for seq, (role, window_id) in enumerate(entries, start=1):
        entry = {
            "schema_version": "research_window_read.v1",
            "question_id": directory.name,
            "seq": seq,
            "read_id": f"{directory.name}-w{seq:04d}",
            "role": role,
            "window_id": window_id,
            "prev_sha256": lg.chain_digest(lines[-1]) if lines else None,
        }
        lines.append(json.dumps(entry, sort_keys=True).encode("utf-8"))
    directory.joinpath("window_reads.jsonl").write_bytes(b"".join(line + b"\n" for line in lines))


def test_the_summary_counts_reads_of_the_validation_window(tmp_path: Path) -> None:
    root = _repo(
        tmp_path, plan=_plan(candidate={"signals": ["tsmom_21"], "screens": ["S0001", "S0002"]})
    )
    _screen(root.parent, family_size=5, signal_snapshot="sig-ffffffffffff")
    validation = "qqq_long_history_validation"
    _reads(
        root.joinpath("QQQ-A"),
        [("validation", validation), ("consistency_check", "exact_three_asset_validated")],
    )
    _reads(root.joinpath("QQQ-B"), [("validation", validation)])
    _reads(
        root.joinpath("QQQ-C"),
        [("validation", "another_window"), ("consistency_check", validation)],
    )
    _reads(root.joinpath("_template"), [("validation", validation)])

    summary = tr.trial_summary(root, "T0001", LOADED)

    assert summary["validation"] == {
        "window_id": validation,
        "reads": 2,
        "next_k": 3,
        "rules": [
            "ci_high.cvar_5pct_relative_improvement > 0",
            "comparison.geometric_annual_return >= -0.01",
        ],
    }
    assert summary["family_size"] == 5
    assert summary["verdict"] == "incomplete"
    assert summary["events"] is None
    assert summary["runs"] == []
    assert (summary["signals"], summary["screens"], summary["primary"]) == (
        ["tsmom_21"],
        ["S0001", "S0002"],
        "primary",
    )


def test_the_report_shows_the_verdict_the_rules_and_the_cost(trial: dict[str, Any]) -> None:
    lines = tr.trial_report(trial["root"], "T0001", LOADED)
    records = trial["records"]
    verdict = {"pass": "过门", "fail": "没过"}[_expected_verdict(records)]

    assert lines[0] == "# 试跑 T0001"
    for line in (
        "- 信号：tsmom_21；筛选记录：S0001（筛选时家族大小 3）",
        "- 假设卡：test card",
        "- 想法来自验证窗口：否",
        f"- 数据：长历史快照 {QLH}；信号快照 {SIG}",
        f"- 判定（主配置 primary）：{verdict}",
        "- 配置 2 个（上限 8）；全局总试验数 2（各题开发试验 0，试跑配置 2）",
        "- 验证窗口 qqq_long_history_validation 已读 0 次；再读是第 1 次，只查方向，"
        "规则同 QQQ 开发窗口：ci_high.cvar_5pct_relative_improvement > 0；"
        "comparison.geometric_annual_return >= -0.01",
        "## primary · QQQ（trial-r0001，ok）",
        "## primary · 美股大盘（trial-r0002，ok）",
        "## half · QQQ（trial-r0003，ok）",
        "| 规则 | 阈值 | 实际 | 成立 |",
        "## 美股大盘快跌段（primary，trial-r0002）",
    ):
        assert line in lines, line
    for record in records:
        for row in record["gate"]:
            observed = "-" if row["observed"] is None else f"{row['observed']:.4f}"
            holds = "是" if row["holds"] else "否"
            line = f"| {row['metric']} | {row['op']} {row['value']:g} | {observed} | {holds} |"
            assert line in lines, line
    for name, row in records[1]["result"]["events"]["episodes"].items():
        cells = [
            name,
            str(row["days"]),
            _percent_text(row["derisked_share"]),
            _percent_text(row["missed_loss"]),
            "-" if row["exit_lag_days"] is None else str(row["exit_lag_days"]),
            "-" if row["reentry_lag_days"] is None else str(row["reentry_lag_days"]),
        ]
        assert "| " + " | ".join(cells) + " |" in lines, name


def _percent_text(value: float | None) -> str:
    return "-" if value is None else f"{value * 100:.1f}%"


def test_the_report_shows_failed_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _repo(tmp_path)
    _error_run(root, monkeypatch)

    def check(*args: Any, **kwargs: Any) -> Any:
        raise BenchCheckError(["look-ahead on 2001-01-02"])

    monkeypatch.setattr(tr, "run_strategy", check)
    tr.run_trial(root, "T0001", "half", "qqq", QQQ, loaded=LOADED)
    monkeypatch.undo()
    lines = tr.trial_report(root, "T0001", LOADED)

    assert "- 判定（主配置 primary）：未完成" in lines
    first = lines.index("## primary · QQQ（trial-r0001，error）")
    assert lines[first + 2] == "出错：RuntimeError: boom"
    second = lines.index("## half · QQQ（trial-r0002，check_failed）")
    assert lines[second + 2] == "检查未通过：look-ahead on 2001-01-02"
    assert not any(line.startswith("## 美股大盘快跌段") for line in lines)


# Commands ----------------------------------------------------------------------------------------


def test_cli_trial_run_records_and_prints(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _repo(tmp_path)
    calls: list[str] = []

    def data(market: str, gate: Any, plan: Any, **kwargs: Any) -> BenchData:
        calls.append(market)
        return QQQ

    monkeypatch.setattr(lg, "research_root", lambda loaded: root)
    monkeypatch.setattr(cli, "load_settings", lambda: LOADED)
    monkeypatch.setattr(tr, "market_data", data)
    result = CliRunner().invoke(
        cli.app, ["trial-run", "T0001", "--config", "primary", "--market", "qqq"]
    )

    assert result.exit_code == 0, result.output
    record = tr.trial_records(root)[0]
    held = sum(row["holds"] for row in record["gate"])
    failed = [row["metric"] for row in record["gate"] if not row["holds"]]
    expected = f"记录 trial-r0001；T0001 primary qqq；状态 ok；门 {held}/2 条成立"
    assert failed
    assert result.output == f"{expected}；没过：{', '.join(failed)}\n"
    assert calls == ["qqq"]


def test_cli_trial_run_refusals_record_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)

    def forbidden(*args: Any, **kwargs: Any) -> BenchData:
        raise AssertionError("no data is loaded for a refused run")

    monkeypatch.setattr(lg, "research_root", lambda loaded: root)
    monkeypatch.setattr(cli, "load_settings", lambda: LOADED)
    monkeypatch.setattr(tr, "market_data", forbidden)
    runner = CliRunner()
    for args in (
        ["trial-run", "T0001", "--config", "primary", "--market", "nasdaq"],
        ["trial-run", "T0009", "--config", "primary", "--market", "qqq"],
        ["trial-run", "T1", "--config", "primary", "--market", "qqq"],
    ):
        result = runner.invoke(cli.app, args)
        assert result.exit_code == 1, args
        assert "试跑拒绝，不记账" in result.output
    monkeypatch.setattr(tr, "market_data", lambda *args, **kwargs: QQQ)
    result = runner.invoke(cli.app, ["trial-run", "T0001", "--config", "nope", "--market", "qqq"])
    assert result.exit_code == 1 and "试跑拒绝，不记账" in result.output
    assert _runs(root) == []


def test_cli_trial_run_failed_run_exits_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(lg, "research_root", lambda loaded: root)
    monkeypatch.setattr(cli, "load_settings", lambda: LOADED)
    monkeypatch.setattr(tr, "market_data", lambda *args, **kwargs: QQQ)
    monkeypatch.setattr(tr, "run_strategy", boom)
    result = CliRunner().invoke(
        cli.app, ["trial-run", "T0001", "--config", "primary", "--market", "qqq"]
    )

    assert result.exit_code == 1
    assert result.output.startswith("记录 trial-r0001；T0001 primary qqq；状态 error")
    assert len(_runs(root)) == 1


def test_cli_trial_ledger_and_report(
    trial: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = trial["root"]
    records = trial["records"]
    monkeypatch.setattr(lg, "research_root", lambda loaded: root)
    monkeypatch.setattr(cli, "load_settings", lambda: LOADED)
    runner = CliRunner()

    result = runner.invoke(cli.app, ["trial-ledger"])
    lines = result.output.splitlines()
    verdict = {"pass": "过门", "fail": "没过"}[_expected_verdict(records)]
    assert result.exit_code == 0, result.output
    assert lines[0] == "试跑账链状态：PASS"
    assert "全局总试验数 2（各题开发试验 0，试跑配置 2）" in lines
    assert f"T0001：配置 2/8；运行 3 条；主配置 primary：{verdict}" in lines
    held = sum(row["holds"] for row in records[0]["gate"])
    assert f"  trial-r0001 primary qqq ok 门 {held}/2" in lines

    result = runner.invoke(cli.app, ["trial-report", "T0001"])
    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[0] == "# 试跑 T0001"
    result = runner.invoke(cli.app, ["trial-report", "T0009"])
    assert result.exit_code == 1 and "试跑报告拒绝" in result.output


def test_cli_trial_ledger_reports_a_broken_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    _error_run(root, monkeypatch)
    root.joinpath("_trials", "runs", "notes.txt").write_text("x", encoding="utf-8")
    monkeypatch.setattr(lg, "research_root", lambda loaded: root)
    monkeypatch.setattr(cli, "load_settings", lambda: LOADED)
    result = CliRunner().invoke(cli.app, ["trial-ledger"])

    assert result.exit_code == 1
    assert result.output.splitlines()[:2] == [
        "试跑账链状态：BROKEN",
        "  问题：runs/notes.txt is not a trial run record",
    ]


# Market data -------------------------------------------------------------------------------------


def test_market_data_builds_each_market_from_the_plan_snapshots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = tr.load_gate(GATE_FILE)
    plan = tr.TrialPlan.model_validate(_plan())
    factors = _factors("1990-01-01", "1990-03-30")
    rate = pd.DataFrame(
        {"value": 5.0}, index=pd.bdate_range("1990-01-01", "1990-03-30", name="date")
    )
    specs = (
        SourceSpec(
            source_id="ff_factors_daily",
            provider="ken_french",
            dataset="F-F_Research_Data_Factors_daily",
            frequency="daily",
            known_lag_trading_days=0,
            description="factors",
        ),
        SourceSpec(
            source_id="dtb3",
            provider="fred",
            dataset="DTB3",
            frequency="daily",
            known_lag_trading_days=1,
            description="bill",
        ),
    )
    snapshot = SignalSnapshot(SIG, {"ff_factors_daily": factors, "dtb3": rate}, specs)
    loads: list[tuple[Path, str]] = []
    benches: list[tuple[Path, str]] = []

    def load_snapshot(root: Path, snapshot_id: str) -> SignalSnapshot:
        loads.append((root, snapshot_id))
        return snapshot

    def from_snapshot(directory: Path, *, qqq_series: str) -> BenchData:
        benches.append((directory, qqq_series))
        return _qqq_data(signal=None)

    monkeypatch.setattr(signal_sources, "load_snapshot", load_snapshot)
    monkeypatch.setattr(tr, "from_snapshot", from_snapshot)
    monkeypatch.setattr(qlh, "resolve_snapshot_dir", lambda root, snapshot_id: root / snapshot_id)

    us = tr.market_data("us_market", gate, plan, snapshot_root=tmp_path, project_root=PROJECT_ROOT)
    assert us.source == {
        "market": "us_market",
        "snapshot_id": SIG,
        "returns_source": "ff_factors_daily",
        "signal_snapshot_id": SIG,
    }
    np.testing.assert_allclose(us.returns["qqq"], factors["Mkt-RF"] + factors["RF"])
    assert np.isnan(us.inputs["source:dtb3:value"].iloc[0])
    assert us.inputs["source:dtb3:value"].iloc[1] == 5.0
    assert benches == []

    qqq = tr.market_data("qqq", gate, plan, snapshot_root=tmp_path, project_root=PROJECT_ROOT)
    assert benches == [(tmp_path / QLH, "actual")]
    assert qqq.source == {"snapshot_id": QLH, "qqq_series": "actual", "signal_snapshot_id": SIG}
    assert "source:ff_factors_daily:Mkt-RF" in qqq.inputs.columns
    assert loads == [(PROJECT_ROOT, SIG), (PROJECT_ROOT, SIG)]


# The research records lane -----------------------------------------------------------------------


def test_the_records_lane_accepts_a_trial_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    top = root.parent
    policy = yaml.safe_load((PROJECT_ROOT / "config/gov008_ship.yaml").read_text(encoding="utf-8"))
    base = _git(top, "rev-parse", "HEAD")
    _error_run(root, monkeypatch)
    _git(top, "switch", "-q", "-c", "records")
    _git(top, "add", "research/_trials/runs/0001.json")
    _git(top, "commit", "-q", "-m", "trial runs")

    assert ship.record_rule("research/_trials/runs/0001.json", policy) is not None
    assert ship.records_problems(top, base, "HEAD", policy, base) == []

    path = root.joinpath("_trials", "runs", "0001.json")
    record = json.loads(path.read_bytes())
    record["prereg_sha256"] = "0" * 64
    path.write_text(json.dumps(record, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    _git(top, "commit", "-q", "-am", "edited record")
    problems = ship.records_problems(top, base, "HEAD", policy, base)
    assert any(
        "prereg_sha256 is not the hash of research/_trials/prereg.yaml" in p for p in problems
    )


def test_the_records_lane_accepts_a_trial_record_from_a_crlf_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A Windows checkout can hold the gate and the plan with CRLF while the commit holds LF; the
    # record reads CRLF as LF, so its hashes are those of the committed files either way.
    root = _repo(tmp_path)
    top = root.parent
    policy = yaml.safe_load((PROJECT_ROOT / "config/gov008_ship.yaml").read_text(encoding="utf-8"))
    base = _git(top, "rev-parse", "HEAD")
    _git(top, "config", "core.autocrlf", "true")
    for name in ("prereg.yaml", "T0001.yaml"):
        path = root.joinpath("_trials", name)
        path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
        rel = f"research/_trials/{name}"
        assert _git(top, "hash-object", "--", rel) == _git(top, "rev-parse", f"HEAD:{rel}")

    record = _error_run(root, monkeypatch)
    committed = {
        name: hashlib.sha256(
            subprocess.run(
                ["git", "show", f"HEAD:research/_trials/{name}"],
                cwd=top,
                check=True,
                capture_output=True,
            ).stdout
        ).hexdigest()
        for name in ("prereg.yaml", "T0001.yaml")
    }
    assert record["strategy"]["module_sha256"] == committed["T0001.yaml"]
    assert record["prereg_sha256"] == committed["prereg.yaml"]
    _git(top, "switch", "-q", "-c", "records")
    _git(top, "add", "research/_trials/runs/0001.json")
    _git(top, "commit", "-q", "-m", "trial runs")
    assert ship.records_problems(top, base, "HEAD", policy, base) == []
