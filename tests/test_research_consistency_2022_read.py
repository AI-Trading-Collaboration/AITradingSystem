"""Acceptance tests for the one read of the 2022 slow bear in QQQ's consistency window
(TRADING-2565-74).

Written before the implementation (period-6 charter sec. 18,
owner_decision:TRADING-2565:2026-10-26:consistency_2022_read). The module reads D, the slow-bear
return of QQQ-Q3's fast_slow (forward record F0001) and slow_only (F0003) minus that of the
constant exposure at the same whole-window average. Order: the development-window check against
the full-precision TRADING-2565-34 numbers, then the ledger registers the read, the window opens
only with that permit, the segment check (exactly one slow bear, back at its high), the readings,
the two recorded consistency runs on the same permit, and only then the output files. The range
is judged on the one-decimal numbers the forward record wrote; the full-precision ends are only
printed. ``check`` is read-only: it never registers, opens the window or simulates it.

Synthetic data only, in a temporary git repository holding a question QQQ-Q3 with two
deterministic strategies (a 21/252-day and a 252-day price sign). The synthetic QQQ rises to a
2021-12 high, falls ten months in 2022 (a slow bear), is back above the high in 2023-11 and ends
mid-month on 2026-06-15; the development window holds two slow bears of its own.
"""

from __future__ import annotations

import dataclasses
import json
import re
import subprocess
from collections.abc import Iterator
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.research_bench import (
    BenchData,
    LedgerError,
    WindowNotOpenError,
    WindowPermit,
    from_daily_frame,
    load_settings,
)
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import segments as sg
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.settings import WindowRole, resolve_window
from ai_trading_system.research_bench.simulate import Simulation, constant_targets, simulate
from ai_trading_system.research_probes import consistency_2022_read as cr

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
Q3_DECISION = "owner_decision:TRADING-2567:2026-10-15:qqq_q3_prereg"
STRATEGY_MODULE = """
def _sign(inputs, days):
    price = inputs["ndx_price"]
    return (price / price.shift(int(days)) > 1.0).astype(float)


def two_signs(inputs, params):
    return (_sign(inputs, params["fast"]) + _sign(inputs, params["slow"])) / 2.0


def one_sign(inputs, params):
    return _sign(inputs, params["slow"])
"""
RULES: list[dict[str, Any]] = [
    {"metric": "comparison.max_drawdown", "op": ">=", "value": 0.05},
    {"metric": "comparison.cvar_5pct_relative_improvement", "op": ">=", "value": 0.1},
]
# Monthly QQQ returns by month range (first, last, return a month), spread evenly over the days.
HEAD = (
    ("1997-01", "2000-02", 0.02),
    ("2000-03", "2001-06", -0.03),
    ("2001-07", "2007-10", 0.012),
    ("2007-11", "2008-08", -0.03),
    ("2008-09", "2021-12", 0.015),
)
PLAN = (*HEAD, ("2022-01", "2022-10", -0.035), ("2022-11", "2026-06", 0.03))
TWO_BEARS = (
    *HEAD,
    ("2022-01", "2022-10", -0.035),
    ("2022-11", "2024-06", 0.03),
    ("2024-07", "2025-02", -0.035),
    ("2025-03", "2026-06", 0.03),
)
NO_DROP = (*HEAD[:-1], ("2008-09", "2026-06", 0.015))
FAST_DROP = (
    *HEAD,
    ("2022-01", "2022-10", -0.035),
    ("2022-11", "2024-12", 0.03),
    ("2025-01", "2025-03", -0.1),
    ("2025-04", "2026-06", 0.03),
)
END = "2026-06-15"
CASH = 0.0001
START = date(2021, 2, 22)
BEAR = ("2022-01", "2022-10")
NAMES = ("average_exposure", "median_difference", "min_difference")


def _data(plan: tuple[tuple[str, str, float], ...] = PLAN, end: str = END) -> BenchData:
    index = pd.bdate_range("1997-01-02", end, name="date")
    months = index.to_period("M")
    daily_rate = np.full(len(index), np.nan)
    for first, last, monthly in plan:
        inside = (months >= pd.Period(first, freq="M")) & (months <= pd.Period(last, freq="M"))
        days = pd.Series(1, index=index[inside]).groupby(months[inside]).transform("size")
        daily_rate[inside] = (1.0 + monthly) ** (1.0 / days.to_numpy(dtype=float)) - 1.0
    level = 1000.0 * np.cumprod(1.0 + daily_rate)
    frame = pd.DataFrame(
        {
            "qqq_proxy_return": daily_rate,
            "cash_return": np.full(len(index), CASH),
            "lev2_return": 2 * daily_rate,
            "lev3_return": 3 * daily_rate,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.025,
        },
        index=index,
    )
    frame.iloc[0, :4] = np.nan
    return from_daily_frame(frame, qqq_series="proxy", source={"snapshot_id": "synthetic-1"})


DATA = _data()


def _prereg(signal_snapshot_id: str | None = None) -> dict[str, Any]:
    def entry(strategy_id: str, function: str, params: dict[str, int]) -> dict[str, Any]:
        return {
            "id": strategy_id,
            "function": function,
            "params": params,
            "rebalance": {"schedule": "month_end"},
            "delay_days": 0,
        }

    data: dict[str, Any] = {"snapshot_id": "synthetic-1", "qqq_series": "proxy"}
    if signal_snapshot_id is not None:
        data["signal_snapshot_id"] = signal_snapshot_id
    return {
        "schema_version": "research_prereg.v1",
        "question_id": cr.QUESTION,
        "question": "Does a fast and a slow trend sign give a shallower drawdown?",
        "question_type": "qqq_direction",
        "owner_decision": Q3_DECISION,
        "data": data,
        "strategies": [
            entry("fast_slow", "two_signs", {"fast": 21, "slow": 252}),
            entry("slow_only", "one_sign", {"slow": 252}),
        ],
        "validation": {
            "strategies": ["fast_slow", "slow_only"],
            "rules": {"fast_slow": RULES, "slow_only": RULES},
        },
        "prior_expectation": {
            "effect": "a shallower drawdown in slow declines",
            "fails_when": "a pullback in an up-trend is followed by a rebound",
            "sources": ["Moskowitz, Ooi and Pedersen 2012"],
        },
        "sample_floor": {"min_return_days": 500},
        "trial_cap": 4,
        "stop_conditions": ["trial cap reached"],
        "production_effect": "none",
        "broker_action": "none",
    }


def _git(cwd: Path, *args: str) -> str:
    done = subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)
    return done.stdout.strip()


def _question(
    top: Path,
    *,
    decision: bool = True,
    validation: bool = True,
    signal_snapshot_id: str | None = None,
) -> lg.Question:
    """QQQ-Q3 approved on main, with the validation read w0001 already registered."""

    directory = top.joinpath("research", cr.QUESTION)
    directory.mkdir(parents=True)
    top.joinpath("docs", "requirements").mkdir(parents=True)
    directory.joinpath("prereg.yaml").write_text(
        yaml.safe_dump(_prereg(signal_snapshot_id), sort_keys=False), encoding="utf-8"
    )
    directory.joinpath("strategy.py").write_text(STRATEGY_MODULE, encoding="utf-8")
    decisions = [Q3_DECISION, *([cr.DECISION] if decision else [])]
    top.joinpath("docs", "requirements", "TRADING-2567.md").write_text(
        "".join(f"- {item}: approved.\n" for item in decisions), encoding="utf-8"
    )
    _git(top, "init", "-q", "-b", "main")
    _git(top, "config", "user.email", "test@example.com")
    _git(top, "config", "user.name", "test")
    _git(top, "add", "-A")
    _git(top, "commit", "-q", "-m", "approve QQQ-Q3")
    question = lg.load_question(directory)
    if validation:
        lg.register_read(question, "validation", LOADED)
    return question


def _plan(question: lg.Question, strategy_id: str) -> tuple[pd.Series, Any]:
    entry = question.prereg.strategy(strategy_id)
    targets = strategy_targets(
        lg.load_strategy(question, strategy_id), DATA.inputs, dict(entry.params)
    )
    return targets, entry.rebalance.to_rebalance()


def _pair(
    question: lg.Question,
    strategy_id: str,
    start: date,
    end: date | None,
    *,
    data: BenchData = DATA,
    fixed_at: str = "window",
) -> tuple[Simulation, Simulation]:
    """The bench's own simulation and its constant exposure, built here without the module."""

    targets, rebalance = _plan(question, strategy_id)
    own = simulate(
        data.returns,
        targets,
        start=start,
        end=end,
        rebalance=rebalance,
        cost_bps=LOADED.cost_bps,
    )
    exposure = float(own.exposure.mean())
    if fixed_at == "segment":
        exposure = float(sg._inside(own.exposure, _bear()).mean())
    fixed = simulate(
        data.returns,
        constant_targets(data.returns.index, exposure),
        start=start,
        end=end,
        rebalance=rebalance,
        cost_bps=LOADED.cost_bps,
    )
    return own, fixed


def _bear() -> sg.Segment:
    return sg.Segment("slow_bear_202112", "slow_bear", *BEAR, "2021-12", "2022-10", 10, None)


def _anchors(question: lg.Question) -> dict[str, dict[str, float]]:
    """The six development-window numbers, computed here with the bench's functions."""

    window = resolve_window(LOADED, "qqq_direction", "development")
    found = sg.segments(sg.window_returns(DATA.returns["qqq"], window))
    out = {}
    for strategy_id in cr.STRATEGIES:
        own, fixed = _pair(question, strategy_id, window.start, window.end)
        summary = sg.slow_bear_summary(sg.segment_rows(own, fixed, found))
        out[strategy_id] = {
            "average_exposure": float(own.exposure.mean()),
            "median_difference": float(summary["median_difference"]),
            "min_difference": float(summary["min_difference"]),
        }
    return out


def _segment_return(returns: pd.Series, first: str, last: str) -> float:
    months = pd.DatetimeIndex(returns.index).to_period("M")
    inside = (months >= pd.Period(first, freq="M")) & (months <= pd.Period(last, freq="M"))
    return float(np.prod(1.0 + returns.to_numpy(dtype=float)[inside])) - 1.0


def _hand_difference(question: lg.Question, strategy_id: str, **kwargs: Any) -> float:
    own, fixed = _pair(question, strategy_id, START, None, **kwargs)
    return _segment_return(own.returns, *BEAR) - _segment_return(fixed.returns, *BEAR)


def _reads_bytes(question: lg.Question) -> bytes:
    return question.directory.joinpath(lg.READS_FILE).read_bytes()


class Spy:
    """Records, in order, the module's ledger calls and every simulation it starts."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, permit: Any = "real") -> None:
        self.events: list[tuple[str, Any]] = []
        self.windows: list[str] = []
        register, record = lg.register_read, lg.run_and_record

        def register_read(question: lg.Question, role: WindowRole, loaded: Any = None) -> Any:
            self.events.append(("register", role))
            if permit != "real":
                return permit
            return register(question, role, loaded)

        def run_and_record(
            question: lg.Question, data: BenchData, strategy_id: str, **kw: Any
        ) -> lg.RecordedRun:
            self.events.append(("record", strategy_id))
            return record(question, data, strategy_id, **kw)

        def simulate_spy(returns: pd.DataFrame, targets: pd.Series, **kw: Any) -> Simulation:
            self.events.append(("simulate", kw["start"]))
            return simulate(returns, targets, **kw)

        def resolve_spy(loaded: Any, question_type: str, role: WindowRole) -> Any:
            self.windows.append(role)
            return resolve_window(loaded, question_type, role)

        monkeypatch.setattr(lg, "register_read", register_read)
        monkeypatch.setattr(lg, "run_and_record", run_and_record)
        monkeypatch.setattr(cr, "simulate", simulate_spy)
        monkeypatch.setattr(cr, "resolve_window", resolve_spy)

    def say(self, text: str) -> None:
        self.events.append(("say", text))

    def kinds(self, kind: str) -> list[int]:
        return [i for i, (name, _) in enumerate(self.events) if name == kind]

    def window_simulations(self) -> list[int]:
        return [
            i
            for i, (name, value) in enumerate(self.events)
            if name == "simulate" and pd.Timestamp(value).date() >= START
        ]


@dataclasses.dataclass
class Done:
    question: lg.Question
    anchors: dict[str, dict[str, float]]
    result: dict[str, Any]
    spy: Spy


@pytest.fixture(scope="module")
def done(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Done]:
    question = _question(tmp_path_factory.mktemp("done"))
    anchors = _anchors(question)
    with pytest.MonkeyPatch.context() as monkeypatch:
        spy = Spy(monkeypatch)
        result = cr.read(question, DATA, LOADED, anchors=anchors, progress=spy.say)
    yield Done(question, anchors, result, spy)


def _numbers_free(value: Any) -> bool:
    return not re.search(r"\d", value)


# 14. Constants -----------------------------------------------------------------------------------


def test_constants() -> None:
    assert cr.RUN_ID == "TRADING-2565-74"
    assert cr.DECISION == "owner_decision:TRADING-2565:2026-10-26:consistency_2022_read"
    assert cr.QUESTION == "QQQ-Q3"
    assert cr.ROLE == "consistency_check"
    assert cr.STRATEGIES == ("fast_slow", "slow_only")
    assert cr.RECORDS == {"fast_slow": "F0001", "slow_only": "F0003"}
    assert cr.TOLERANCE == 1e-12
    assert cr.ANCHORS == {
        "fast_slow": {
            "average_exposure": 0.5941523375781023,
            "median_difference": 0.09223942070915914,
            "min_difference": 0.06567862091761645,
        },
        "slow_only": {
            "average_exposure": 0.6557873485868102,
            "median_difference": 0.18687386804531436,
            "min_difference": 0.13144606813888593,
        },
    }
    assert cr.RANGES == {"fast_slow": (0.026, 0.120), "slow_only": (-0.048, 0.167)}
    assert cr.FULL_RANGES == {
        "fast_slow": (0.02633206433891666, 0.11970601274143344),
        "slow_only": (-0.04777164845950099, 0.1669990867122531),
    }
    assert cr.LABEL == "一致性窗口 2022 段（旧任务用过、走势已知）"
    assert cr.RECORD_NOTE == "只记、不改 QQQ-Q3 的结论"
    assert (cr.SUMMARY, cr.REPORT) == ("consistency_2022_read.json", "consistency_2022_read.md")


def test_the_module_calls_the_bench_and_names_the_role_once() -> None:
    source = Path(cr.__file__).read_text(encoding="utf-8")
    for name in (
        "simulate",
        "segments",
        "segment_rows",
        "register_read",
        "run_and_record",
        "open_window",
        "resolve_window",
        "benchmark_simulations",
        "summarize",
    ):
        assert f"def {name}(" not in source
    assert source.count('"consistency_check"') == 1
    assert "slow_bear_check" not in source and "forward_records" not in source


# 1. The development-window check ---------------------------------------------------------------


def test_the_check_matches_the_same_path_computed_with_the_bench(done: Done) -> None:
    checked = done.result["dev_check"]
    assert checked["passed"] is True
    assert checked["window"]["role"] == "development"
    assert [(row["strategy"], row["name"]) for row in checked["rows"]] == [
        (strategy_id, name) for strategy_id in cr.STRATEGIES for name in NAMES
    ]
    for row in checked["rows"]:
        assert abs(row["value"] - done.anchors[row["strategy"]][row["name"]]) <= 1e-12
        assert row["anchor"] == done.anchors[row["strategy"]][row["name"]]
        assert row["passed"] is True


@pytest.mark.parametrize(
    ("strategy_id", "name"), [("fast_slow", NAMES[0]), ("slow_only", NAMES[2])]
)
def test_an_anchor_off_by_two_e_minus_twelve_stops_before_registering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, strategy_id: str, name: str
) -> None:
    question = _question(tmp_path)
    anchors = _anchors(question)
    anchors[strategy_id][name] += 2e-12
    reads = _reads_bytes(question)
    spy = Spy(monkeypatch)
    result = cr.read(question, DATA, LOADED, anchors=anchors, progress=spy.say)
    assert result["stage"] == "dev_check"
    assert result["dev_check"]["passed"] is False
    assert [row["passed"] for row in result["dev_check"]["rows"]].count(False) == 1
    assert spy.kinds("register") == [] and spy.window_simulations() == []
    assert _reads_bytes(question) == reads
    assert result["read_id"] is None and result["segments"] is None and result["runs"] == {}
    assert result["complete"] is False
    assert all(item["verdict"] == "incomplete" for item in result["readings"].values())
    assert set(result["readings"]["fast_slow"]) == {"record", "verdict"}
    assert ("say", "开发窗口核对 不过") in spy.events


def test_a_signal_snapshot_stops_before_registering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    question = _question(tmp_path, signal_snapshot_id="sig-0123456789ab")
    spy = Spy(monkeypatch)
    result = cr.read(question, DATA, LOADED, anchors=cr.ANCHORS, progress=spy.say)
    assert result["stage"] == "dev_check" and result["read_id"] is None
    assert spy.kinds("register") == [] and spy.window_simulations() == []
    assert len(result["errors"]) == 1 and "ValueError" in result["errors"][0]
    assert result["complete"] is False


def test_the_decision_must_be_on_main_before_registering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    question = _question(tmp_path, decision=False)
    reads = _reads_bytes(question)
    spy = Spy(monkeypatch)
    result = cr.read(question, DATA, LOADED, anchors=_anchors(question), progress=spy.say)
    assert result["dev_check"]["passed"] is True
    assert result["stage"] == "register" and result["read_id"] is None
    assert spy.kinds("register") == [] and spy.window_simulations() == []
    assert _reads_bytes(question) == reads
    assert len(result["errors"]) == 1 and "docs/requirements" in result["errors"][0]


# 2. The window opens only with the permit of a registered read ----------------------------------


@pytest.mark.parametrize(
    ("permit", "error"),
    [
        (None, WindowNotOpenError),
        (
            WindowPermit("QQQ-Q3", "consistency_check", "QQQ-Q3-w0009", ("fast_slow", "slow_only")),
            LedgerError,
        ),
    ],
)
def test_the_window_opens_only_with_a_registered_permit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, permit: Any, error: type[Exception]
) -> None:
    question = _question(tmp_path)
    reads = _reads_bytes(question)
    spy = Spy(monkeypatch, permit=permit)
    result = cr.read(question, DATA, LOADED, anchors=_anchors(question), progress=spy.say)
    assert spy.kinds("register") != []
    assert result["stage"] == "segments"
    assert len(result["errors"]) == 1 and error.__name__ in result["errors"][0]
    assert result["segments"] is None and result["runs"] == {}
    assert spy.window_simulations() == [] and spy.kinds("record") == []
    assert _reads_bytes(question) == reads
    assert not question.directory.joinpath(lg.RUNS_DIR).exists()
    assert all(item["verdict"] == "incomplete" for item in result["readings"].values())


def test_the_consistency_role_never_goes_through_resolve_window(done: Done) -> None:
    assert done.spy.windows != []
    assert set(done.spy.windows) == {"development"}


# 3. Order ---------------------------------------------------------------------------------------


def test_registering_comes_before_any_simulation_of_the_window(done: Done) -> None:
    spy = done.spy
    (register,) = spy.kinds("register")
    window = spy.window_simulations()
    assert window != []
    assert register < min(window)
    assert all(
        pd.Timestamp(value).date() < START
        for name, value in spy.events[:register]
        if name == "simulate"
    )


def test_the_runs_are_recorded_after_the_readings(done: Done) -> None:
    spy = done.spy
    records = spy.kinds("record")
    assert [spy.events[i][1] for i in records] == ["fast_slow", "slow_only"]
    assert max(spy.window_simulations()) < min(records)
    assert spy.events.index(("say", "读数算完")) < min(records)
    said = [value for name, value in spy.events if name == "say"]
    assert said[:4] == ["开发窗口核对 过", "登记 QQQ-Q3-w0002", "划段 过", "读数算完"]
    assert said[4:] == ["记账 QQQ-Q3-r0001", "记账 QQQ-Q3-r0002"]


# 4. The segment check ---------------------------------------------------------------------------


def test_one_recovered_slow_bear_passes(done: Done) -> None:
    result = done.result
    assert result["stage"] == "done" and result["complete"] is True
    assert result["read_id"] == "QQQ-Q3-w0002"
    assert result["window"] == {
        "window_id": "exact_three_asset_validated",
        "role": "consistency_check",
        "start": "2021-02-22",
        "end": None,
        "data_end": END,
    }
    cut = result["segments"]
    assert cut["passed"] is True and cut["slow_bears"] == 1
    assert (cut["first_month"], cut["last_month"]) == ("2021-02", "2026-06")
    assert cut["rows"] == [
        {
            "id": "slow_bear_202112",
            "kind": "slow_bear",
            "start": "2022-01",
            "end": "2022-10",
            "peak": "2021-12",
            "trough": "2022-10",
            "recovered": "2023-11",
            "months": 10,
        },
        {
            "id": "long_bull_202211",
            "kind": "long_bull",
            "start": "2022-11",
            "end": "2026-06",
            "peak": None,
            "trough": None,
            "recovered": None,
            "months": 44,
        },
    ]


@pytest.mark.parametrize(
    ("plan", "end", "bears"),
    [(TWO_BEARS, END, 2), (PLAN, "2023-06-30", 1), (NO_DROP, END, 0)],
    ids=["two_slow_bears", "not_back_at_the_high", "no_drop"],
)
def test_a_failed_segment_check_reads_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    plan: tuple[tuple[str, str, float], ...],
    end: str,
    bears: int,
) -> None:
    question = _question(tmp_path)
    anchors = _anchors(question)
    spy = Spy(monkeypatch)
    result = cr.read(question, _data(plan, end), LOADED, anchors=anchors, progress=spy.say)
    assert result["stage"] == "segments" and result["read_id"] == "QQQ-Q3-w0002"
    assert result["segments"]["passed"] is False
    assert result["segments"]["slow_bears"] == bears
    assert spy.window_simulations() == [] and spy.kinds("record") == []
    assert result["runs"] == {} and result["errors"] == []
    assert all(item["verdict"] == "incomplete" for item in result["readings"].values())
    assert ("say", "划段 不过") in spy.events
    for row in result["segments"]["rows"]:
        assert set(row) == {"id", "kind", "start", "end", "peak", "trough", "recovered", "months"}
    if end != END:
        assert result["segments"]["rows"][0]["recovered"] is None
        assert "没回到" in cr.render(result)
    assert "## 慢熊段读数" not in cr.render(result)


def test_a_fast_drop_beside_the_slow_bear_is_only_printed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    question = _question(tmp_path)
    result = cr.read(question, _data(FAST_DROP), LOADED, anchors=_anchors(question))
    assert result["stage"] == "done" and result["complete"] is True
    kinds = [(row["kind"], row["start"], row["end"]) for row in result["segments"]["rows"]]
    assert kinds == [("slow_bear", "2022-01", "2022-10"), ("fast_drop", "2025-01", "2025-03")]
    after = result["readings"]["fast_slow"]["outside"][1]
    assert (after["kind"], after["start"], after["end"], after["months"]) == (
        "post_trough",
        "2022-11",
        "2026-06",
        44,
    )


def test_the_segmentation_reads_qqq_not_the_strategy(done: Done) -> None:
    # QQQ falls 30% in 2022; neither strategy loses 20% there, so only QQQ shows the slow bear.
    for strategy_id in cr.STRATEGIES:
        assert done.result["readings"][strategy_id]["strategy_return"] > -0.2
    assert abs(done.result["readings"]["fast_slow"]["qqq_return"] - (0.965**10 - 1.0)) <= 1e-12


def test_recovery_month_is_the_first_month_back_at_the_high() -> None:
    months = pd.period_range("2021-11", "2023-02", freq="M")
    values = [1.0, 1.2, 1.0, 0.9, 0.8, 0.85, 0.9, 1.0, 1.1, 1.19, 1.2, 1.3, 1.1, 1.0, 1.25, 1.4]
    level = pd.Series(values, index=months)
    bear = sg.drop_segments(level)[0]
    assert (bear.peak, bear.trough) == ("2021-12", "2022-03")
    # Equal to the high counts as back at it (segments._drops uses >=).
    assert cr.recovery_month(level, bear) == "2022-09"
    assert cr.recovery_month(level.iloc[:10], bear) is None


# 5., 3a. D and the cash -------------------------------------------------------------------------


@pytest.mark.parametrize("strategy_id", cr.STRATEGIES)
def test_d_is_the_hand_computed_difference_at_the_window_average(
    done: Done, strategy_id: str
) -> None:
    item = done.result["readings"][strategy_id]
    hand = _hand_difference(done.question, strategy_id)
    assert abs(item["difference"] - hand) <= 1e-12
    assert abs(item["difference"] - (item["strategy_return"] - item["benchmark_return"])) <= 1e-12
    own, _ = _pair(done.question, strategy_id, START, None)
    assert item["window_exposure"] == float(own.exposure.mean())
    assert item["segment_exposure"] == float(sg._inside(own.exposure, _bear()).mean())
    # The constant exposure is the whole window's average, not the segment's.
    assert abs(hand - _hand_difference(done.question, strategy_id, fixed_at="segment")) > 1e-3


@pytest.mark.parametrize("strategy_id", cr.STRATEGIES)
def test_the_part_not_invested_earns_the_cash_return(done: Done, strategy_id: str) -> None:
    no_cash = dataclasses.replace(DATA, returns=DATA.returns.assign(cash=0.0))
    without = _hand_difference(done.question, strategy_id, data=no_cash)
    assert abs(done.result["readings"][strategy_id]["difference"] - without) > 1e-3


# 6., 3f. The verdicts ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("difference", "positive"), [(0.0001, True), (0.0, False), (-0.0001, False)]
)
def test_d_above_zero_is_right(difference: float, positive: bool) -> None:
    verdict = cr.verdicts(difference, cr.RANGES["slow_only"], cr.FULL_RANGES["slow_only"])
    assert verdict["positive"] is positive


@pytest.mark.parametrize(
    ("difference", "inside", "between"),
    [
        (0.026, True, True),
        (0.120, True, True),
        (0.0262, True, True),
        (0.1198, True, True),
        (0.05, True, False),
        (0.0259, False, False),
        (0.1201, False, False),
        (-0.01, False, False),
    ],
)
def test_the_range_is_the_written_one_decimal_range_with_both_ends(
    difference: float, inside: bool, between: bool
) -> None:
    verdict = cr.verdicts(difference, cr.RANGES["fast_slow"], cr.FULL_RANGES["fast_slow"])
    assert verdict["in_range"] is inside
    assert verdict["between"] is between


def test_the_readings_carry_both_verdicts_and_both_ranges(done: Done) -> None:
    for strategy_id in cr.STRATEGIES:
        item = done.result["readings"][strategy_id]
        low, high = cr.RANGES[strategy_id]
        assert item["record"] == cr.RECORDS[strategy_id] and item["verdict"] == "read"
        assert item["range"] == [low, high]
        assert item["full_range"] == list(cr.FULL_RANGES[strategy_id])
        assert item["positive"] is (item["difference"] > 0)
        assert item["in_range"] is (low <= item["difference"] <= high)


# 7., 3e. The segments outside the slow bear -----------------------------------------------------


def test_outside_the_bear_the_long_bull_runs_to_the_mid_month_data_end(done: Done) -> None:
    for strategy_id in cr.STRATEGIES:
        before, after = done.result["readings"][strategy_id]["outside"]
        assert (before["kind"], before["start"], before["end"], before["months"]) == (
            "pre_peak",
            "2021-02",
            "2021-12",
            11,
        )
        assert before["unfinished"] is False and "annual_difference" not in before
        assert (after["kind"], after["start"], after["end"], after["months"]) == (
            "long_bull",
            "2022-11",
            "2026-06",
            44,
        )
        assert after["unfinished"] is True and isinstance(after["annual_difference"], float)
        own, fixed = _pair(done.question, strategy_id, START, None)
        hand = _segment_return(own.returns, "2021-02", "2021-12") - _segment_return(
            fixed.returns, "2021-02", "2021-12"
        )
        assert abs(before["difference"] - hand) <= 1e-12
        last = own.returns.index[-1]
        assert pd.Timestamp(last).date().isoformat() == END


def test_a_short_post_trough_is_not_a_long_bull() -> None:
    daily = sg.window_returns(_data(end="2024-12-31").returns["qqq"], _consistency())
    level = sg.monthly_level(daily)
    found = sg.segments(daily)
    bear = next(item for item in found if item.kind == "slow_bear")
    before, after = cr.outside_segments(level, bear, found)
    assert (before.kind, before.start, before.end, before.months) == (
        "pre_peak",
        "2021-02",
        "2021-12",
        11,
    )
    assert (after.kind, after.start, after.end, after.months) == (
        "post_trough",
        "2022-11",
        "2024-12",
        26,
    )


def _consistency() -> Any:
    return resolve_window(LOADED, "qqq_direction", "consistency_check")


# 3b. Signals read the history before the window -------------------------------------------------


def test_the_signals_use_the_history_before_the_window(done: Done) -> None:
    # Both signs are positive at the window start only through the 252 days before it.
    for strategy_id in cr.STRATEGIES:
        before = done.result["readings"][strategy_id]["outside"][0]
        assert abs(before["average_exposure"] - 1.0) <= 1e-12
        months = done.result["readings"][strategy_id]["monthly_exposure"]
        assert months[0] == {"month": "2022-01", "exposure": 1.0}
        assert [row["month"] for row in months] == [
            str(m) for m in pd.period_range(*BEAR, freq="M")
        ]


# 8. The whole window ----------------------------------------------------------------------------


@pytest.mark.parametrize("strategy_id", cr.STRATEGIES)
def test_the_whole_window_log_difference_and_summaries(done: Done, strategy_id: str) -> None:
    whole = done.result["readings"][strategy_id]["window"]
    own, fixed = _pair(done.question, strategy_id, START, None)
    logs = float(np.log1p(own.returns).sum() - np.log1p(fixed.returns).sum())
    assert abs(whole["log_difference"] - logs) <= 1e-12
    for side, sim in (("strategy", own), ("benchmark", fixed)):
        equity = float(np.prod(1.0 + sim.returns.to_numpy(dtype=float)))
        years = (sim.returns.index[-1] - sim.start).days / 365.25
        assert abs(whole[side]["geometric_annual_return"] - (equity ** (1 / years) - 1)) <= 1e-12
    qqq = done.result["readings"][strategy_id]["window"]["qqq"]
    assert qqq["average_exposure"] == 1.0
    assert abs(qqq["max_drawdown"] - (0.965**10 - 1.0)) <= 1e-9


# 9., 3a. The recorded runs ----------------------------------------------------------------------


def test_the_two_runs_are_recorded_on_the_same_permit(done: Done) -> None:
    directory = done.question.directory
    reads = lg.window_reads(directory)
    assert [(entry["read_id"], entry["role"]) for entry in reads] == [
        ("QQQ-Q3-w0001", "validation"),
        ("QQQ-Q3-w0002", "consistency_check"),
    ]
    records = lg.run_records(directory)
    assert [
        (r["run_id"], r["strategy"]["id"], r["window_read_id"], r["window"]["role"], r["status"])
        for r in records
    ] == [
        ("QQQ-Q3-r0001", "fast_slow", "QQQ-Q3-w0002", "consistency_check", "ok"),
        ("QQQ-Q3-r0002", "slow_only", "QQQ-Q3-w0002", "consistency_check", "ok"),
    ]
    assert lg.verify_reads(directory) == [] and lg.verify_runs(directory) == []
    for record in records:
        run = done.result["runs"][record["strategy"]["id"]]
        assert run["run_id"] == record["run_id"] and run["status"] == "ok"
        assert run["rules"] == record["rules"] and len(run["rules"]) == len(RULES)
        assert run["note"] == cr.RECORD_NOTE


@pytest.mark.parametrize("strategy_id", cr.STRATEGIES)
def test_the_recorded_run_agrees_with_the_readings_on_its_own(done: Done, strategy_id: str) -> None:
    record = next(
        r for r in lg.run_records(done.question.directory) if r["strategy"]["id"] == strategy_id
    )
    whole = done.result["readings"][strategy_id]["window"]
    strategy = record["result"]["strategy"]
    for name in ("average_exposure", "geometric_annual_return", "max_drawdown"):
        assert abs(strategy[name] - whole["strategy"][name]) <= 1e-12
    matched = record["result"]["benchmarks"]["matched_constant"]
    assert (
        abs(matched["geometric_annual_return"] - whole["benchmark"]["geometric_annual_return"])
        <= 1e-12
    )
    assert (
        done.result["readings"][strategy_id]["window_exposure"]
        == whole["strategy"]["average_exposure"]
    )
    run = done.result["runs"][strategy_id]
    assert run["consistent"] is True
    assert [row["name"] for row in run["rows"]] == [
        "strategy.average_exposure",
        "strategy.geometric_annual_return",
        "strategy.max_drawdown",
        "benchmark.geometric_annual_return",
    ]


def test_a_record_that_disagrees_is_printed_and_changes_no_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, done: Done
) -> None:
    question = _question(tmp_path)
    record = lg.run_and_record

    def shifted(*args: Any, **kwargs: Any) -> lg.RecordedRun:
        out = record(*args, **kwargs)
        out.record["result"]["strategy"]["average_exposure"] += 1e-9
        return out

    monkeypatch.setattr(lg, "run_and_record", shifted)
    result = cr.read(question, DATA, LOADED, anchors=done.anchors)
    for strategy_id in cr.STRATEGIES:
        assert result["runs"][strategy_id]["consistent"] is False
        mine, base = result["readings"][strategy_id], done.result["readings"][strategy_id]
        for key in ("difference", "positive", "in_range", "between", "verdict"):
            assert mine[key] == base[key]
    assert result["complete"] is True and result["errors"] == []
    assert "不一致" in cr.render(result)


# 10. A run that raises --------------------------------------------------------------------------


def test_a_run_that_raises_keeps_the_verdicts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, done: Done
) -> None:
    question = _question(tmp_path)
    record = lg.run_and_record

    def broken(question: lg.Question, data: BenchData, strategy_id: str, **kw: Any) -> Any:
        if strategy_id == "fast_slow":
            raise RuntimeError("bench broke at 0.0731 on 2022-10-31")
        return record(question, data, strategy_id, **kw)

    monkeypatch.setattr(lg, "run_and_record", broken)
    monkeypatch.setattr(cr, "load", lambda root=None: (LOADED, question, DATA))
    monkeypatch.setattr(cr, "ANCHORS", done.anchors)
    out = tmp_path.joinpath("out")
    invoked = CliRunner().invoke(cr.app, ["run", "--out", str(out)])
    assert invoked.exit_code == 0, invoked.output
    result = json.loads(out.joinpath(cr.SUMMARY).read_text(encoding="utf-8"))
    assert result["complete"] is True
    for strategy_id in cr.STRATEGIES:
        assert result["readings"][strategy_id]["verdict"] == "read"
    run = result["runs"]["fast_slow"]
    assert run["status"] == "error" and run["error_type"] == "RuntimeError"
    assert _numbers_free(run["error"])
    assert result["runs"]["slow_only"]["status"] == "ok"
    assert len(result["errors"]) == 1 and _numbers_free(result["errors"][0])
    assert "RuntimeError" in out.joinpath(cr.REPORT).read_text(encoding="utf-8")


def test_an_error_while_reading_one_strategy_leaves_it_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, done: Done
) -> None:
    question = _question(tmp_path)
    rows = sg.segment_rows
    calls: list[date] = []

    def broken(strategy: Simulation, benchmark: Simulation, items: Any) -> Any:
        # The first call on the consistency window is fast_slow's slow-bear row.
        if pd.Timestamp(strategy.start).date() >= START:
            calls.append(pd.Timestamp(strategy.start).date())
            if len(calls) == 1:
                raise ValueError("D 0.0731 in 2022-10")
        return rows(strategy, benchmark, items)

    monkeypatch.setattr(sg, "segment_rows", broken)
    result = cr.read(question, DATA, LOADED, anchors=done.anchors)
    assert result["stage"] == "done" and result["complete"] is False
    assert len(result["errors"]) >= 1
    assert all(_numbers_free(error) for error in result["errors"])
    assert result["readings"]["fast_slow"] == {"record": "F0001", "verdict": "incomplete"}
    assert result["readings"]["slow_only"]["verdict"] == "read"
    assert [run["status"] for run in result["runs"].values()] == ["ok", "ok"]
    assert result["runs"]["fast_slow"]["consistent"] is None


# 11. Errors carry no numbers --------------------------------------------------------------------


def test_error_text_drops_every_number() -> None:
    text = cr.error_text(ValueError("D -0.0731 (1.5e-3) at 2022-10-31 in w0002\nsecond 7"))
    assert text.startswith("ValueError: D ")
    assert _numbers_free(text) and "second" not in text


def test_an_error_in_the_check_has_no_numbers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    question = _question(tmp_path)

    def broken(rows: Any) -> Any:
        raise ValueError("median 0.0922 of 2 segments from 2000-03")

    monkeypatch.setattr(sg, "slow_bear_summary", broken)
    result = cr.read(question, DATA, LOADED, anchors=cr.ANCHORS)
    assert result["stage"] == "dev_check" and result["read_id"] is None
    assert len(result["errors"]) == 1 and _numbers_free(result["errors"][0])


# 12., 3. The command ----------------------------------------------------------------------------


def test_the_command_writes_the_files_last_and_prints_no_numbers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, done: Done
) -> None:
    question = _question(tmp_path)
    out = tmp_path.joinpath("out")
    register, record = lg.register_read, lg.run_and_record

    def register_read(*args: Any, **kwargs: Any) -> Any:
        assert not out.exists()
        return register(*args, **kwargs)

    def run_and_record(*args: Any, **kwargs: Any) -> Any:
        assert not out.exists()
        return record(*args, **kwargs)

    monkeypatch.setattr(lg, "register_read", register_read)
    monkeypatch.setattr(lg, "run_and_record", run_and_record)
    monkeypatch.setattr(cr, "load", lambda root=None: (LOADED, question, DATA))
    monkeypatch.setattr(cr, "ANCHORS", done.anchors)
    invoked = CliRunner().invoke(cr.app, ["run", "--out", str(out)])
    assert invoked.exit_code == 0, invoked.output
    assert sorted(path.name for path in out.iterdir()) == sorted((cr.SUMMARY, cr.REPORT))
    assert not re.search(r"\d\.\d", invoked.output)
    assert "登记 QQQ-Q3-w0002" in invoked.output
    assert "fast_slow（F0001）D > 0 " in invoked.output
    result = json.loads(out.joinpath(cr.SUMMARY).read_text(encoding="utf-8"))
    assert result["base"]["commit"]
    report = out.joinpath(cr.REPORT).read_text(encoding="utf-8")
    assert cr.LABEL in report and cr.RECORD_NOTE in report and "（没完）" in report
    item = result["readings"]["fast_slow"]
    assert f"D = {100 * item['difference']:+.2f} 点" in report


def test_an_incomplete_read_exits_one(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    question = _question(tmp_path)
    monkeypatch.setattr(cr, "load", lambda root=None: (LOADED, question, DATA))
    out = tmp_path.joinpath("out")
    invoked = CliRunner().invoke(cr.app, ["run", "--out", str(out)])
    assert invoked.exit_code == 1, invoked.output
    result = json.loads(out.joinpath(cr.SUMMARY).read_text(encoding="utf-8"))
    assert result["stage"] == "dev_check" and result["read_id"] is None


def test_an_out_directory_that_is_not_empty_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    question = _question(tmp_path)
    reads = _reads_bytes(question)
    out = tmp_path.joinpath("out")
    out.mkdir()
    out.joinpath("old.txt").write_text("x", encoding="utf-8")
    monkeypatch.setattr(lg, "register_read", lambda *a, **k: pytest.fail("registered"))
    monkeypatch.setattr(cr, "load", lambda root=None: pytest.fail("loaded"))
    invoked = CliRunner().invoke(cr.app, ["run", "--out", str(out)])
    assert invoked.exit_code == 2
    assert [path.name for path in out.iterdir()] == ["old.txt"]
    assert _reads_bytes(question) == reads


# Read-only check --------------------------------------------------------------------------------


def test_check_never_registers_opens_or_simulates_the_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    question = _question(tmp_path)
    anchors = _anchors(question)
    reads = _reads_bytes(question)
    spy = Spy(monkeypatch)
    monkeypatch.setattr(cr, "open_window", lambda *a, **k: pytest.fail("opened"))
    result = cr.check(question, DATA, LOADED, anchors=anchors)
    assert result["passed"] is True and result["errors"] == []
    assert result["dev_check"]["passed"] is True
    assert result["ledger"] == {
        "passed": True,
        "problems": [],
        "reads": 1,
        "next_read": "QQQ-Q3-w0002",
    }
    assert result["data"] == {
        "start": "2020-02-01",
        "columns": {
            "qqq": {
                "days": int((DATA.returns.index >= "2020-02-01").sum()),
                "missing": 0,
                "last": END,
            },
            "cash": {
                "days": int((DATA.returns.index >= "2020-02-01").sum()),
                "missing": 0,
                "last": END,
            },
            # The signals read ndx_return from the inputs, not the returns.
            "ndx_return": {
                "days": int((DATA.inputs.index >= "2020-02-01").sum()),
                "missing": 0,
                "last": END,
            },
        },
        "passed": True,
    }
    assert spy.kinds("register") == [] and spy.window_simulations() == []
    assert set(spy.windows) == {"development"}
    assert _reads_bytes(question) == reads
    assert not question.directory.joinpath(lg.RUNS_DIR).exists()


def test_check_counts_a_missing_day_and_a_missing_validation_read(tmp_path: Path) -> None:
    question = _question(tmp_path, validation=False)
    returns = DATA.returns.copy()
    returns.loc[pd.Timestamp("2022-03-01"), "qqq"] = np.nan
    inputs = DATA.inputs.copy()
    inputs.loc[pd.Timestamp("2021-06-01"), "ndx_return"] = np.nan
    inputs.loc[pd.Timestamp("2021-06-02"), "ndx_return"] = np.nan
    result = cr.check(
        question,
        dataclasses.replace(DATA, returns=returns, inputs=inputs),
        LOADED,
        anchors=_anchors(question),
    )
    assert result["passed"] is False
    assert result["data"]["columns"]["qqq"]["missing"] == 1
    assert result["data"]["columns"]["cash"]["missing"] == 0
    assert result["data"]["columns"]["ndx_return"]["missing"] == 2
    assert result["data"]["passed"] is False
    assert result["ledger"]["passed"] is False and result["ledger"]["next_read"] == "QQQ-Q3-w0001"
    assert not question.directory.joinpath(lg.READS_FILE).exists()


def test_check_command_exit_codes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    question = _question(tmp_path)
    monkeypatch.setattr(cr, "load", lambda root=None: (LOADED, question, DATA))
    invoked = CliRunner().invoke(cr.app, ["check"])
    assert invoked.exit_code == 1 and "check 不过" in invoked.output
    monkeypatch.setattr(cr, "ANCHORS", _anchors(question))
    invoked = CliRunner().invoke(cr.app, ["check"])
    assert invoked.exit_code == 0, invoked.output
    assert "下一个读取号 QQQ-Q3-w0002" in invoked.output and "check 过" in invoked.output
    assert len(lg.window_reads(question.directory)) == 1
