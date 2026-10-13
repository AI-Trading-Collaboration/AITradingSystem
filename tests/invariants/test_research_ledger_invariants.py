"""Invariants of the trial ledger and window reads (TRADING-2565 S3b).

Multiple testing is only controlled if every run is counted and the validation window is read once,
for a question whose rules were fixed first. Each guard has a negative control.

11. Every run is recorded: a run that fails its checks and a run that raises are recorded too.
12. Records are chained: a changed or removed record is found, and nothing is appended to a broken
    chain.
13. Trials are counted: a repeat is not a new trial, new parameters or new strategy code are; a new
    trial beyond the cap is refused before it runs and leaves no record.
14. The validation window opens once per question, only for an approved preregistration (the
    prereg and strategy module on main, the owner decision in main's docs/requirements), with the
    preregistered parameters, one run per validated strategy; the consistency window only after it.
15. Records hold summaries only, no daily series.
16. The deflated Sharpe ratio matches the paper's worked example and a hand computation.
17. In this repository, every question's records and reads are intact, git history never changed
    or removed one, and a decision cites only recorded runs and registered reads.
22. Validation and consistency runs always face the standard falsification (S3c), and their rules
    are judged on it; a rule naming a metric no run has refuses the read before it is registered.
    (18-21, the falsification itself: ``test_research_falsify_invariants.py``.)
"""

from __future__ import annotations

import json
import math
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import (
    BenchCheckError,
    BenchData,
    LedgerError,
    RunSpec,
    TrialCapError,
    WindowNotOpenError,
    WindowPermit,
    from_daily_frame,
    load_question,
    load_settings,
    register_read,
    run_and_record,
    run_strategy,
)
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench.deflated import (
    ReturnMoments,
    deflated_sharpe,
    expected_max_sharpe,
    probabilistic_sharpe,
)

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
QUESTION = "QQQ-T1"
DECISION = "owner_decision:TRADING-2567:2026-10-13:qqq_t1_prereg"
STRATEGY_MODULE = """
import numpy as np
import pandas as pd


def vol_target(inputs, params):
    vol = inputs["qqq_return"].rolling(int(params["window"])).std() * np.sqrt(252)
    return (float(params["target"]) / vol).clip(upper=float(params["cap"]))


def leaky(inputs, params):
    return (inputs["qqq_return"].shift(-1) > 0).astype(float) * float(params["cap"])


def broken(inputs, params):
    raise RuntimeError("strategy bug")
"""


RULES: list[dict[str, Any]] = [
    {"metric": "comparison.max_drawdown", "op": ">=", "value": 0.05},
    {"metric": "comparison.cvar_5pct_relative_improvement", "op": ">=", "value": 0.1},
]


def _prereg(
    cap: int = 3, decision: str | None = DECISION, rules: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    return {
        "schema_version": "research_prereg.v1",
        "question_id": QUESTION,
        "question": "Does volatility-managed exposure cut the left tail at a matched exposure?",
        "question_type": "qqq_direction",
        "owner_decision": decision,
        "data": {"snapshot_id": "synthetic-1", "qqq_series": "proxy"},
        "strategies": [
            {
                "id": "q1a",
                "function": "vol_target",
                "params": {"window": 21, "target": 0.2, "cap": 1.0},
                "rebalance": {"schedule": "month_end"},
            },
            {"id": "leak", "function": "leaky", "params": {"cap": 1.0}},
            {"id": "bug", "function": "broken", "params": {}},
        ],
        "validation": {
            "strategies": ["q1a"],
            "rules": {"q1a": RULES if rules is None else rules},
        },
        "prior_expectation": {
            "effect": "lower drawdown and left tail, similar geometric return",
            "fails_when": "volatility jumps without warning",
            "sources": ["Moreira and Muir 2017"],
        },
        "sample_floor": {"min_return_days": 500},
        "trial_cap": cap,
        "stop_conditions": ["trial cap reached"],
        "production_effect": "none",
        "broker_action": "none",
    }


def _git(cwd: Path, *args: str) -> str:
    done = subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)
    return done.stdout.strip()


def _repo(tmp_path: Path, **prereg: Any) -> Path:
    top = tmp_path.joinpath("repo")
    directory = top.joinpath("research", QUESTION)
    directory.mkdir(parents=True)
    top.joinpath("docs", "requirements").mkdir(parents=True)
    directory.joinpath("prereg.yaml").write_text(
        yaml.safe_dump(_prereg(**prereg), sort_keys=False), encoding="utf-8"
    )
    directory.joinpath("strategy.py").write_text(STRATEGY_MODULE, encoding="utf-8")
    _git(top, "init", "-q", "-b", "main")
    _git(top, "config", "user.email", "test@example.com")
    _git(top, "config", "user.name", "test")
    return directory


def _approve(directory: Path) -> None:
    top = directory.parents[1]
    top.joinpath("docs", "requirements", "TRADING-2567.md").write_text(
        f"- {DECISION}: QQQ-T1 preregistration approved.\n", encoding="utf-8"
    )
    _git(top, "add", "-A")
    _git(top, "commit", "-q", "-m", "approve QQQ-T1")


def _data(seed: int = 3) -> BenchData:
    index = pd.bdate_range("1997-01-02", "2022-06-30", name="date")
    rng = np.random.default_rng(seed)
    r = rng.normal(0.0004, 0.015, len(index))
    level = 1000.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "cash_return": np.full(len(index), 0.0001),
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
    daily.iloc[0, :4] = np.nan
    return from_daily_frame(daily, qqq_series="proxy", source={"snapshot_id": "synthetic-1"})


DATA = _data()


def _records(directory: Path) -> list[dict[str, Any]]:
    return lg.run_records(directory)


# 11 ----------------------------------------------------------------------------------------------


def test_every_run_is_recorded_including_failed_ones(tmp_path: Path) -> None:
    question = load_question(_repo(tmp_path))
    run_and_record(question, DATA, "q1a", loaded=LOADED)
    with pytest.raises(BenchCheckError):
        run_and_record(question, DATA, "leak", loaded=LOADED)
    with pytest.raises(RuntimeError, match="strategy bug"):
        run_and_record(question, DATA, "bug", loaded=LOADED)

    records = _records(question.directory)
    assert [record["status"] for record in records] == ["ok", "check_failed", "error"]
    assert [record["run_id"] for record in records] == [f"{QUESTION}-r000{i}" for i in (1, 2, 3)]
    assert records[1]["problems"] and records[2]["error"].startswith("RuntimeError: strategy bug")
    # Failed runs are trials too.
    assert [record["trial"]["number"] for record in records] == [1, 2, 3]


# 12 ----------------------------------------------------------------------------------------------


def test_a_changed_or_removed_record_breaks_the_chain(tmp_path: Path) -> None:
    question = load_question(_repo(tmp_path, cap=10))
    for target in (0.2, 0.25, 0.3):
        run_and_record(question, DATA, "q1a", params={"target": target}, loaded=LOADED)
    assert lg.verify_runs(question.directory) == []

    runs = question.directory.joinpath("runs")
    first = runs.joinpath("0001.json")
    original = first.read_bytes()
    record = json.loads(original)
    record["result"]["strategy"]["max_drawdown"] = -0.01
    first.write_text(json.dumps(record, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    assert lg.verify_runs(question.directory) == [
        "runs/0002.json: prev_sha256 does not match the record before it"
    ]
    with pytest.raises(LedgerError, match="broken"):
        run_and_record(question, DATA, "q1a", loaded=LOADED)

    first.write_bytes(original.replace(b"\n", b"\r\n"))  # line endings alone change nothing
    assert lg.verify_runs(question.directory) == []
    runs.joinpath("0002.json").unlink()
    assert lg.verify_runs(question.directory) == [
        "runs/0003.json: expected record 0002; a record is missing",
        "runs/0003.json: prev_sha256 does not match the record before it",
    ]


# 13 ----------------------------------------------------------------------------------------------


def test_trials_are_counted_and_capped(tmp_path: Path) -> None:
    question = load_question(_repo(tmp_path, cap=2))
    first = run_and_record(question, DATA, "q1a", loaded=LOADED).record
    repeat = run_and_record(question, DATA, "q1a", loaded=LOADED).record
    second = run_and_record(question, DATA, "q1a", params={"target": 0.3}, loaded=LOADED).record
    assert (first["trial"]["number"], first["trial"]["new"]) == (1, True)
    assert (repeat["trial"]["number"], repeat["trial"]["new"]) == (1, False)
    assert (second["trial"]["number"], second["trial"]["new"]) == (2, True)

    with pytest.raises(TrialCapError):
        run_and_record(question, DATA, "q1a", params={"target": 0.4}, loaded=LOADED)
    assert len(_records(question.directory)) == 3  # refused before it ran: no record
    # A repeat at the cap is still allowed.
    run_and_record(question, DATA, "q1a", params={"target": 0.3}, loaded=LOADED)

    # New strategy code is a new trial even with the same id and parameters.
    module = question.strategy_path
    module.write_text(module.read_text(encoding="utf-8") + "\n# changed\n", encoding="utf-8")
    with pytest.raises(TrialCapError):
        run_and_record(load_question(question.directory), DATA, "q1a", loaded=LOADED)


# 14 ----------------------------------------------------------------------------------------------


def test_validation_needs_an_approved_prereg(tmp_path: Path) -> None:
    directory = _repo(tmp_path)
    question = load_question(directory)
    with pytest.raises(LedgerError, match="not on main|not approved"):
        register_read(question, "validation", LOADED)

    top = directory.parents[1]
    _git(top, "add", "-A")
    _git(top, "commit", "-q", "-m", "prereg without the decision in docs/requirements")
    with pytest.raises(LedgerError, match="is not in docs/requirements on main"):
        register_read(question, "validation", LOADED)

    _approve(directory)
    prereg = directory.joinpath("prereg.yaml")
    prereg.write_text(prereg.read_text(encoding="utf-8") + "# edited\n", encoding="utf-8")
    with pytest.raises(LedgerError, match="differs from the version on main"):
        register_read(load_question(directory), "validation", LOADED)
    assert not directory.joinpath(lg.READS_FILE).exists()

    _git(top, "checkout", "-q", "--", ".")
    permit = register_read(load_question(directory), "validation", LOADED)
    assert permit.read_id == f"{QUESTION}-w0001" and permit.strategies == ("q1a",)


def test_the_validation_window_is_read_once(tmp_path: Path) -> None:
    directory = _repo(tmp_path)
    _approve(directory)
    question = load_question(directory)
    with pytest.raises(LedgerError, match="only after the validation read"):
        register_read(question, "consistency_check", LOADED)
    forged = WindowPermit(QUESTION, "validation", f"{QUESTION}-w0001", ("q1a",))
    with pytest.raises(LedgerError, match="not registered"):
        run_and_record(question, DATA, "q1a", role="validation", permit=forged, loaded=LOADED)
    with pytest.raises(LedgerError, match="needs the permit"):
        run_and_record(question, DATA, "q1a", role="validation", loaded=LOADED)
    spec = RunSpec("qqq_direction", "q1a", {"window": 21, "target": 0.2, "cap": 1.0}, "validation")
    with pytest.raises(WindowNotOpenError):  # the bench itself refuses without the ledger
        run_strategy(DATA, lg.load_strategy(question, "q1a"), spec, LOADED)

    permit = register_read(question, "validation", LOADED)
    assert permit == forged
    with pytest.raises(LedgerError, match="preregistered parameters only"):
        run_and_record(
            question,
            DATA,
            "q1a",
            role="validation",
            params={"target": 0.3},
            permit=permit,
            loaded=LOADED,
        )
    with pytest.raises(LedgerError, match="does not cover"):
        run_and_record(question, DATA, "leak", role="validation", permit=permit, loaded=LOADED)
    record = run_and_record(
        question, DATA, "q1a", role="validation", permit=permit, loaded=LOADED
    ).record
    assert record["window"]["role"] == "validation" and record["trial"] is None
    assert [rule["metric"] for rule in record["rules"]] == [
        "comparison.max_drawdown",
        "comparison.cvar_5pct_relative_improvement",
    ]
    with pytest.raises(LedgerError, match="already ran"):
        run_and_record(question, DATA, "q1a", role="validation", permit=permit, loaded=LOADED)
    with pytest.raises(LedgerError, match="read once per question"):
        register_read(question, "validation", LOADED)

    consistency = register_read(question, "consistency_check", LOADED)
    run_and_record(
        question, DATA, "q1a", role="consistency_check", permit=consistency, loaded=LOADED
    )
    assert [entry["role"] for entry in lg.window_reads(directory)] == [
        "validation",
        "consistency_check",
    ]


# 15 ----------------------------------------------------------------------------------------------


def _largest(value: Any) -> int:
    """The most entries in any list or mapping inside a record."""

    if isinstance(value, dict):
        return max([len(value), *(_largest(item) for item in value.values())])
    if isinstance(value, list):
        return max([len(value), *(_largest(item) for item in value)])
    return 0


def test_records_hold_summaries_only(tmp_path: Path) -> None:
    question = load_question(_repo(tmp_path))
    run_and_record(question, DATA, "q1a", loaded=LOADED)
    run_and_record(question, DATA, "q1a", loaded=LOADED, falsify=True)
    plain, falsified = (
        question.directory.joinpath("runs", name) for name in ("0001.json", "0002.json")
    )
    for path, size in ((plain, 20_000), (falsified, 80_000)):
        record = json.loads(path.read_bytes())
        assert _largest(record) < 60
        assert path.stat().st_size < size
    # Negative controls: a daily series, as a list or keyed by date, is caught.
    record["result"]["daily"] = list(range(len(DATA.returns)))
    assert _largest(record) >= 60
    record["result"]["daily"] = {str(day.date()): 0.0 for day in DATA.returns.index}
    assert _largest(record) >= 60


# 16 ----------------------------------------------------------------------------------------------


def test_deflated_sharpe_matches_the_worked_example() -> None:
    # Bailey and Lopez de Prado (2014), section 4: 100 trials, variance of annualized Sharpe ratios
    # 1/2, best annualized Sharpe 2.5 over 1,250 daily observations, skewness -3, kurtosis 10.
    variance = 0.5 / 250
    moments = ReturnMoments(2.5 / math.sqrt(250), -3.0, 10.0, 1250)
    assert expected_max_sharpe(100, variance) == pytest.approx(0.1132, abs=5e-5)
    assert deflated_sharpe(moments, 100, variance) == pytest.approx(0.9004, abs=5e-5)
    # By hand, one trial: SR0 = 0 and PSR = Phi(0.05 * 10 / sqrt(1 + 0.5 * 0.0025)).
    one = ReturnMoments(0.05, 0.0, 3.0, 101)
    z = 0.05 * 10 / math.sqrt(1 + 0.5 * 0.05**2)
    assert deflated_sharpe(one, 1, 0.0) == pytest.approx(0.5 * (1 + math.erf(z / math.sqrt(2))))
    # Negative control: more trials deflate more.
    assert deflated_sharpe(moments, 1000, variance) < deflated_sharpe(moments, 100, variance)
    assert probabilistic_sharpe(moments, 0.0) > deflated_sharpe(moments, 100, variance)


# 17 ----------------------------------------------------------------------------------------------


def test_repository_ledgers_are_intact_and_only_ever_appended() -> None:
    root_dir = lg.research_root(LOADED)
    problems = []
    for directory in lg.question_directories(root_dir):
        problems += [f"{directory.name}: {p}" for p in lg.verify_runs(directory)]
        problems += [f"{directory.name}: {p}" for p in lg.verify_reads(directory)]
        problems += [f"{directory.name}: {p}" for p in lg.admission_problems(directory)]
    if lg.git_output(PROJECT_ROOT, "rev-parse", "--show-toplevel") is not None:
        problems += lg.history_problems(PROJECT_ROOT, LOADED.settings.research_root)
    assert problems == []


def test_history_and_admission_checks_catch_rewrites(tmp_path: Path) -> None:
    directory = _repo(tmp_path, cap=10)
    _approve(directory)
    question = load_question(directory)
    top = directory.parents[1]
    run_and_record(question, DATA, "q1a", loaded=LOADED)
    permit = register_read(question, "validation", LOADED)
    run_and_record(question, DATA, "q1a", role="validation", permit=permit, loaded=LOADED)
    directory.joinpath("decision.md").write_text(
        f"Kept: {QUESTION}-r0002 (read {QUESTION}-w0001).\n", encoding="utf-8"
    )
    _git(top, "add", "-A")
    _git(top, "commit", "-q", "-m", "runs")
    assert lg.history_problems(top) == [] and lg.admission_problems(directory) == []

    directory.joinpath("decision.md").write_text(f"Kept: {QUESTION}-r0009.\n", encoding="utf-8")
    assert lg.admission_problems(directory) == [
        f"decision.md cites {QUESTION}-r0009, which is not a recorded run"
    ]
    reads = directory.joinpath(lg.READS_FILE)
    reads.write_text(reads.read_text(encoding="utf-8").replace("w0001", "w0007"), encoding="utf-8")
    record = directory.joinpath("runs", "0001.json")
    record.write_text(record.read_text(encoding="utf-8") + " ", encoding="utf-8")
    _git(top, "add", "-A")
    _git(top, "commit", "-q", "-m", "rewrite")
    problems = lg.history_problems(top)
    assert f"git history changes or removes the run record research/{QUESTION}/runs/0001.json" in (
        problems
    )
    assert any("rewrites earlier lines" in problem for problem in problems)


# 22 ----------------------------------------------------------------------------------------------


def test_validation_and_consistency_runs_always_face_the_falsification(tmp_path: Path) -> None:
    unknown = {"metric": "cost_30.max_drawdown", "op": ">=", "value": 0.0}
    directory = _repo(tmp_path, rules=[*RULES, unknown])
    _approve(directory)
    with pytest.raises(LedgerError, match="no metric cost_30.max_drawdown"):
        register_read(load_question(directory), "validation", LOADED)
    assert lg.window_reads(directory) == []

    falsified_rules = [
        *RULES,
        {"metric": "ci_low.max_drawdown", "op": ">", "value": -1.0},
        {"metric": "placebo_rank.max_drawdown", "op": ">=", "value": 0.0},
        {"metric": "cost_25.geometric_annual_return", "op": "<", "value": 1.0},
        {"metric": "leave_year_min.cvar_5pct_relative_improvement", "op": ">", "value": -10.0},
    ]
    directory.joinpath("prereg.yaml").write_text(
        yaml.safe_dump(_prereg(rules=falsified_rules), sort_keys=False), encoding="utf-8"
    )
    _approve(directory)
    question = load_question(directory)
    development = run_and_record(question, DATA, "q1a", loaded=LOADED).record
    assert development["result"]["falsification"] is None

    permit = register_read(question, "validation", LOADED)
    record = run_and_record(
        question, DATA, "q1a", role="validation", permit=permit, loaded=LOADED, falsify=False
    ).record
    falsification = record["result"]["falsification"]
    assert record["falsification_confidence"] == LOADED.settings.falsification.confidence
    assert falsification["settings"]["placebo_draws"] == 10
    observed = {rule["metric"]: rule["observed"] for rule in record["rules"]}
    assert observed["ci_low.max_drawdown"] == falsification["ci_low"]["max_drawdown"]
    assert observed["placebo_rank.max_drawdown"] == falsification["placebo_rank"]["max_drawdown"]
    assert observed["cost_25.geometric_annual_return"] == (
        falsification["cost_25"]["geometric_annual_return"]
    )
    assert all(isinstance(value, float) for value in observed.values())
    assert [rule["holds"] for rule in record["rules"]][2:] == [True, True, True, True]

    consistency = register_read(question, "consistency_check", LOADED)
    record = run_and_record(
        question, DATA, "q1a", role="consistency_check", permit=consistency, loaded=LOADED
    ).record
    assert record["result"]["falsification"]["placebo_rank"]
