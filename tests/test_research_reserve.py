"""Acceptance tests for the bench's reserve and the P1 trial (TRADING-2565-21).

The reserve is what the strategy holds in place of cash when it holds less than the full exposure:
cash, as before, or a synthetic 10-year Treasury built from DGS10
(owner_decision:TRADING-2567:2026-10-18:portfolio_p1). The comparison stays QQQ plus cash. The
trial plan T0002 tries a fixed 60/40 mix under the two-stage ruler; a US-market run with the
Treasury uses the 1962-2010 window because DGS10 starts in 1962. Every test uses synthetic data.
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
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import run as rn
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.checks import (
    check_no_lookahead,
    check_parameters_consumed,
)
from ai_trading_system.research_bench.data import (
    RESERVES,
    TREASURY_RESERVE,
    TREASURY_SOURCE,
    BenchData,
    from_daily_frame,
    from_us_market,
    strategy_returns,
    treasury_returns,
    with_reserves,
    with_signal_sources,
)
from ai_trading_system.research_bench.falsify import FalsificationInputs, falsify, paired_fields
from ai_trading_system.research_bench.fixed_strategy import fixed_exposure
from ai_trading_system.research_bench.metrics import compare, summarize
from ai_trading_system.research_bench.run import RunSpec, run_strategy, spec_record
from ai_trading_system.research_bench.settings import load_settings, resolve_window
from ai_trading_system.research_bench.simulate import Rebalance, constant_targets, simulate
from ai_trading_system.research_bench.sources import SourceSpec
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_data.signal_sources import SignalSnapshot

FULL = load_settings()
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
DECISION = "owner_decision:TRADING-2567:2026-10-18:portfolio_p1"
GATE_DECISION = "owner_decision:TRADING-2565:2026-10-19:tail_floor10"
GATE_FILE = PROJECT_ROOT / "research/_trials/prereg.yaml"
PLAN_FILE = PROJECT_ROOT / "research/_trials/T0002.yaml"
SIG = "sig-0123456789ab"
QLH = "qlh-test-snapshot"
MONTHLY = Rebalance(schedule="month_end")


def _par_bond_price(coupon: float, rate: float) -> float:
    """Ten-year bond paying coupon/2 twice a year, discounted at rate/2 per half year."""

    half = 1.0 + rate / 2.0
    return sum((coupon / 2.0) / half**k for k in range(1, 21)) + 1.0 / half**20


def _yields(index: pd.DatetimeIndex, seed: int = 11) -> pd.DataFrame:
    """A FRED-like DGS10 frame: percent, one column named value."""

    steps = np.random.default_rng(seed).normal(0.0, 0.04, len(index))
    level = np.clip(6.0 + np.cumsum(steps), 1.0, 15.0)
    return pd.DataFrame({"value": level}, index=index)


# Synthetic Treasury -------------------------------------------------------------------------------


def test_constant_yields_earn_the_coupon_by_calendar_days() -> None:
    calendar = pd.DatetimeIndex(["2001-01-04", "2001-01-05", "2001-01-08", "2001-01-09"])
    yields = pd.Series(5.0, index=calendar)

    out = treasury_returns(yields, calendar)

    assert out.name == TREASURY_RESERVE == "treasury_10y"
    assert out.index.equals(calendar)
    assert math.isnan(out.iloc[0])
    assert out.iloc[1:].tolist() == pytest.approx(
        [0.05 / 365, 0.05 * 3 / 365, 0.05 / 365], abs=1e-14
    )


def test_a_yield_change_reprices_a_ten_year_par_bond() -> None:
    calendar = pd.DatetimeIndex(["2001-01-04", "2001-01-05", "2001-01-08"])
    yields = pd.Series([5.0, 5.1, 4.8], index=calendar)

    out = treasury_returns(yields, calendar)

    first = _par_bond_price(0.05, 0.051) - 1 + 0.05 / 365
    second = _par_bond_price(0.051, 0.048) - 1 + 0.051 * 3 / 365
    assert out.iloc[1] == pytest.approx(first, abs=1e-12)
    assert out.iloc[2] == pytest.approx(second, abs=1e-12)
    # About -0.78% for a 0.1-point rise at 5%: the duration of a ten-year par bond, not a bill.
    assert first == pytest.approx(-0.0078, abs=0.0003)


def test_yields_are_taken_as_of_each_bench_date() -> None:
    calendar = pd.DatetimeIndex(["2001-01-04", "2001-01-05", "2001-01-08", "2001-01-09"])
    # Missing on 01-05 (FRED "."), and an observation on Saturday 01-06 that the bench never sees.
    yields = pd.Series(
        [5.0, np.nan, 5.2, 5.3],
        index=pd.DatetimeIndex(["2001-01-04", "2001-01-05", "2001-01-06", "2001-01-09"]),
    )

    out = treasury_returns(yields, calendar)

    assert out.iloc[1] == pytest.approx(0.05 / 365, abs=1e-14)  # unchanged yield: coupon only
    assert out.iloc[2] == pytest.approx(
        _par_bond_price(0.05, 0.052) - 1 + 0.05 * 3 / 365, abs=1e-12
    )
    assert out.iloc[3] == pytest.approx(_par_bond_price(0.052, 0.053) - 1 + 0.052 / 365, abs=1e-12)


def test_stale_and_missing_yields_give_no_return() -> None:
    calendar = pd.bdate_range("2001-01-01", periods=12)
    yields = pd.Series(5.0, index=calendar[:3])  # nothing after the third date

    out = treasury_returns(yields, calendar)

    # As-of for five more bench dates (the signal sources' staleness limit), then nothing.
    assert out.iloc[1:8].notna().all()
    assert out.iloc[8:].isna().all()
    before = pd.Series(5.0, index=calendar[5:])
    assert treasury_returns(before, calendar).iloc[:6].isna().all()


@pytest.mark.parametrize("bad", [0.05, 0.0, -1.0, 30.0])
def test_yields_must_be_percent(bad: float) -> None:
    calendar = pd.bdate_range("2001-01-01", periods=5)
    yields = pd.Series([5.0, 5.0, bad, 5.0, 5.0], index=calendar)
    with pytest.raises(ValueError, match="percent"):
        treasury_returns(yields, calendar)


# Reserves on bench data -------------------------------------------------------------------------


def _qqq_data(seed: int = 3, start: str = "1997-01-02") -> BenchData:
    index = pd.bdate_range(start, "2011-06-30", name="date")
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
    return from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": QLH})


def _factors(start: str = "1926-07-01", end: str = "2011-06-30", seed: int = 5) -> pd.DataFrame:
    index = pd.bdate_range(start, end, name="date")
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {"Mkt-RF": rng.normal(0.0003, 0.011, len(index)), "RF": 0.0001}, index=index
    )


DGS10 = _yields(pd.bdate_range("1962-01-02", "2011-06-30"))
QQQ = with_reserves(
    with_signal_sources(_qqq_data(), {}, {}, snapshot_id=SIG), {TREASURY_SOURCE: DGS10}
)
US = with_reserves(
    with_signal_sources(
        from_us_market({"ff_factors_daily": _factors()}, snapshot_id=SIG), {}, {}, snapshot_id=SIG
    ),
    {TREASURY_SOURCE: DGS10},
)


def test_bench_data_has_no_reserve_until_one_is_added() -> None:
    plain = _qqq_data()
    assert dict(plain.reserves) == {}
    assert dict(from_us_market({"ff_factors_daily": _factors()}, snapshot_id=SIG).reserves) == {}
    assert with_reserves(plain, {"ff_factors_daily": _factors()}).reserves == plain.reserves
    assert RESERVES == ("cash", "treasury_10y")


def test_with_reserves_adds_the_treasury_on_the_bench_calendar() -> None:
    plain = with_signal_sources(_qqq_data(), {}, {}, snapshot_id=SIG)
    data = with_reserves(plain, {TREASURY_SOURCE: DGS10})

    assert set(data.reserves) == {TREASURY_RESERVE}
    expected = treasury_returns(DGS10["value"], pd.DatetimeIndex(plain.returns.index))
    pd.testing.assert_series_equal(data.reserves[TREASURY_RESERVE], expected)
    pd.testing.assert_frame_equal(data.returns, plain.returns)
    pd.testing.assert_frame_equal(data.inputs, plain.inputs)
    assert data.source == plain.source


def test_signal_sources_keep_the_reserves() -> None:
    again = with_signal_sources(QQQ, {}, {}, snapshot_id="sig-ba9876543210")
    assert set(again.reserves) == {TREASURY_RESERVE}
    pd.testing.assert_series_equal(again.reserves[TREASURY_RESERVE], QQQ.reserves[TREASURY_RESERVE])


def test_the_strategy_trades_its_reserve_in_place_of_cash() -> None:
    assert strategy_returns(QQQ, "cash").equals(QQQ.returns)
    traded = strategy_returns(QQQ, TREASURY_RESERVE)

    assert list(traded.columns) == list(QQQ.returns.columns)
    pd.testing.assert_frame_equal(
        traded.drop(columns="cash"), QQQ.returns.drop(columns="cash"), check_names=False
    )
    assert traded["cash"].equals(QQQ.reserves[TREASURY_RESERVE].rename("cash"))
    assert QQQ.returns["cash"].eq(0.0001).all()  # the data's own cash is untouched
    with pytest.raises(ValueError, match="unknown reserve"):
        strategy_returns(QQQ, "gold")
    with pytest.raises(ValueError, match="no treasury_10y reserve"):
        strategy_returns(_qqq_data(), TREASURY_RESERVE)


# Fixed exposure ---------------------------------------------------------------------------------


def test_fixed_exposure_holds_one_number() -> None:
    targets = fixed_exposure(QQQ.inputs, {"exposure": 0.6})
    assert targets.index.equals(QQQ.inputs.index)
    assert targets.dtype == float
    assert (targets == 0.6).all()
    assert fixed_exposure(QQQ.inputs, {"exposure": 1}).eq(1.0).all()


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"exposure": 0.6, "x": 1},
        {"level": 0.6},
        {"exposure": True},
        {"exposure": "0.6"},
        {"exposure": math.nan},
    ],
)
def test_fixed_exposure_refuses_other_parameters(params: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="exposure"):
        fixed_exposure(QQQ.inputs, params)


def test_fixed_exposure_passes_the_bench_checks() -> None:
    index = pd.DatetimeIndex(QQQ.inputs.index)
    start, end = pd.Timestamp("1999-03-10"), pd.Timestamp("2010-12-31")
    cuts = list(index[(index > start) & (index < end)][::500])
    assert check_no_lookahead(fixed_exposure, QQQ.inputs, {"exposure": 0.6}, cuts) == []
    assert check_parameters_consumed(fixed_exposure, QQQ.inputs, {"exposure": 0.6}, start, end) == (
        [],
        [],
    )


# Runs with a reserve ----------------------------------------------------------------------------


def _spec(reserve: str = TREASURY_RESERVE, exposure: float = 0.6, **changes: Any) -> RunSpec:
    fields: dict[str, Any] = {
        "question_type": "qqq_direction",
        "strategy_id": "mix",
        "params": {"exposure": exposure},
        "window_role": "development",
        "rebalance": MONTHLY,
        "reserve": reserve,
    }
    return RunSpec(**{**fields, **changes})


def _same(left: Any, right: Any) -> bool:
    """Equal as recorded: the same JSON once NaN becomes null, as the ledgers write it."""

    return json.dumps(lg._plain(left), sort_keys=True) == json.dumps(
        lg._plain(right), sort_keys=True
    )


@pytest.fixture(scope="module")
def runs() -> dict[str, Any]:
    return {
        "bond": run_strategy(QQQ, fixed_exposure, _spec(), LOADED, falsification_confidence=0.95),
        "cash": run_strategy(
            QQQ, fixed_exposure, _spec("cash"), LOADED, falsification_confidence=0.95
        ),
    }


def _window_sim(returns: pd.DataFrame, exposure: float, cost: float | None = None) -> Any:
    return simulate(
        returns,
        constant_targets(returns.index, exposure),
        start=pd.Timestamp("1999-03-10").date(),
        end=pd.Timestamp("2010-12-31").date(),
        rebalance=MONTHLY,
        cost_bps=LOADED.cost_bps if cost is None else cost,
    )


def test_the_spec_records_the_reserve_only_when_it_is_not_cash() -> None:
    plain = spec_record(_spec("cash"))
    assert RunSpec("qqq_direction", "mix", {}, "development").reserve == "cash"
    assert set(plain) == {
        "question_type",
        "strategy_id",
        "params",
        "window_role",
        "rebalance",
        "delay_days",
        "leveraged_benchmarks",
    }
    assert spec_record(_spec()) == {**plain, "reserve": "treasury_10y"}


def test_the_strategy_holds_the_reserve_and_the_benchmarks_hold_cash(runs: dict[str, Any]) -> None:
    bond, cash = runs["bond"], runs["cash"]
    cash_returns = QQQ.returns["cash"]
    held = _window_sim(strategy_returns(QQQ, TREASURY_RESERVE), 0.6)

    assert _same(bond.strategy, summarize(held, cash_returns, LOADED))
    matched = _window_sim(QQQ.returns, float(held.exposure.mean()))
    assert _same(bond.benchmarks["matched_constant"], summarize(matched, cash_returns, LOADED))
    assert _same(bond.benchmarks["buy_and_hold_qqq"], cash.benchmarks["buy_and_hold_qqq"])
    assert _same(bond.benchmarks["cash"], cash.benchmarks["cash"])
    assert _same(
        bond.comparison, compare(bond.strategy, bond.benchmarks["matched_constant"], LOADED)
    )
    assert bond.spec["reserve"] == "treasury_10y" and "reserve" not in cash.spec
    assert bond.fingerprint != cash.fingerprint
    assert bond.strategy["geometric_annual_return"] != cash.strategy["geometric_annual_return"]


def test_falsification_reruns_the_strategy_on_its_reserve(runs: dict[str, Any]) -> None:
    bond = runs["bond"]
    traded = strategy_returns(QQQ, TREASURY_RESERVE)
    held = _window_sim(traded, 0.6, cost=25.0)
    matched = _window_sim(QQQ.returns, float(held.exposure.mean()), cost=25.0)
    expected = compare(
        summarize(held, QQQ.returns["cash"], LOADED),
        summarize(matched, QQQ.returns["cash"], LOADED),
        LOADED,
    )
    falsification = bond.falsification
    assert falsification is not None
    assert _same(falsification["cost_25"], expected)
    # A fixed exposure has no timing: every same-exposure random timing is the strategy itself.
    ranks = falsification["placebo_rank"]
    assert {ranks[field] for field in paired_fields(LOADED.settings)} == {0.5}


def test_falsify_without_strategy_returns_is_unchanged() -> None:
    sim = _window_sim(QQQ.returns, 0.6)
    matched = _window_sim(QQQ.returns, float(sim.exposure.mean()))
    inputs = FalsificationInputs(
        returns=QQQ.returns,
        targets=constant_targets(QQQ.returns.index, 0.6),
        start=pd.Timestamp("1999-03-10").date(),
        end=pd.Timestamp("2010-12-31").date(),
        rebalance=MONTHLY,
        delay_days=0,
        cost_bps=LOADED.cost_bps,
        strategy=sim,
        benchmark=matched,
    )
    assert inputs.strategy_returns is None
    same = replace(inputs, strategy_returns=QQQ.returns)
    assert _same(falsify(inputs, LOADED, 0.95), falsify(same, LOADED, 0.95))


def test_a_reserve_the_data_lack_or_miss_inside_the_window_stops_the_run() -> None:
    with pytest.raises(ValueError, match="no treasury_10y reserve"):
        run_strategy(_qqq_data(), fixed_exposure, _spec(), LOADED)
    with pytest.raises(ValueError, match="unknown reserve"):
        run_strategy(QQQ, fixed_exposure, _spec("gold"), LOADED)
    gap = QQQ.reserves[TREASURY_RESERVE].copy()
    gap.loc[pd.Timestamp("2005-06-15")] = np.nan
    holed = replace(QQQ, reserves={TREASURY_RESERVE: gap})
    with pytest.raises(ValueError, match="reserve treasury_10y has no return on 2005-06-15"):
        run_strategy(holed, fixed_exposure, _spec(), LOADED)


def test_the_fingerprint_leaves_out_the_diagnostics(runs: dict[str, Any]) -> None:
    for result in runs.values():
        payload = {
            "spec": result.spec,
            "window": result.window,
            "source": result.source,
            "settings_sha256": result.settings_sha256,
            "cost_bps": result.cost_bps,
            "strategy": result.strategy,
            "benchmarks": result.benchmarks,
        }
        assert rn._fingerprint(payload) == result.fingerprint
        assert "matched_volatility" not in result.benchmarks
        assert _same(result.to_dict()["diagnostics"], result.diagnostics)


def test_matched_volatility_is_qqq_and_cash_at_the_strategy_volatility(
    runs: dict[str, Any],
) -> None:
    for result in runs.values():
        diagnostic = result.diagnostics["matched_volatility"]
        ratio = (
            result.strategy["annual_volatility"]
            / result.benchmarks["buy_and_hold_qqq"]["annual_volatility"]
        )
        assert diagnostic["exposure"] == pytest.approx(min(1.0, ratio), abs=1e-12)
        sim = _window_sim(QQQ.returns, diagnostic["exposure"])
        summary = summarize(sim, QQQ.returns["cash"], LOADED)
        assert _same(diagnostic["summary"], summary)
        assert _same(diagnostic["comparison"], compare(result.strategy, summary, LOADED))


def test_matched_volatility_of_a_constant_mix_with_cash_is_itself() -> None:
    # Daily rebalancing and a constant cash return: half QQQ has exactly half QQQ's volatility.
    spec = _spec("cash", exposure=0.5, rebalance=Rebalance(), cost_bps=0.0)
    result = run_strategy(QQQ, fixed_exposure, spec, LOADED)
    diagnostic = result.diagnostics["matched_volatility"]

    assert diagnostic["exposure"] == pytest.approx(0.5, abs=1e-9)
    assert diagnostic["comparison"]["geometric_annual_return"] == pytest.approx(0.0, abs=1e-9)
    assert diagnostic["comparison"]["max_drawdown"] == pytest.approx(0.0, abs=1e-9)


def test_matched_volatility_stops_at_full_exposure() -> None:
    spec = _spec("cash", exposure=2.0, leveraged_benchmarks=True)
    result = run_strategy(QQQ, fixed_exposure, spec, LOADED)
    assert result.diagnostics["matched_volatility"]["exposure"] == 1.0


# The gate, the window and the plan ---------------------------------------------------------------


def test_the_us_market_uses_the_1962_window_with_the_treasury() -> None:
    gate = tr.load_gate(GATE_FILE)

    assert gate.owner_decision == GATE_DECISION
    assert gate.markets["us_market"].reserve_question_types == {
        "treasury_10y": "us_market_bond_trial"
    }
    assert gate.markets["qqq"].reserve_question_types is None
    assert tr.gate_problems(gate, FULL) == []
    window = resolve_window(FULL, "us_market_bond_trial", "development")
    assert (window.window_id, window.start.isoformat(), window.end) == (
        "us_market_1962_2010",
        "1962-01-02",
        pd.Timestamp("2010-12-31").date(),
    )
    for role in ("validation", "consistency_check"):
        with pytest.raises(KeyError):
            resolve_window(FULL, "us_market_bond_trial", role)


@pytest.mark.parametrize(
    "mapping", [{"cash": "us_market_trial"}, {"gold": "us_market_trial"}, {"treasury_10y": ""}]
)
def test_reserve_question_types_name_a_reserve_and_a_type(mapping: dict[str, str]) -> None:
    gate = yaml.safe_load(GATE_FILE.read_text(encoding="utf-8"))
    gate["markets"]["us_market"]["reserve_question_types"] = mapping
    with pytest.raises(ValueError):
        tr.TrialGate.model_validate(gate)


def test_a_reserve_type_with_no_window_is_a_gate_problem() -> None:
    gate = yaml.safe_load(GATE_FILE.read_text(encoding="utf-8"))
    gate["markets"]["us_market"]["reserve_question_types"] = {"treasury_10y": "no_such_type"}
    problems = tr.gate_problems(tr.TrialGate.model_validate(gate), FULL)
    assert len(problems) == 1
    assert problems[0].startswith("us_market: ") and "no_such_type" in problems[0]


def _fixed(config_id: str, exposure: float, **changes: Any) -> dict[str, Any]:
    return {
        "id": config_id,
        "function": "fixed_exposure",
        "params": {"exposure": exposure},
        "reserve": "treasury_10y",
        "rebalance": {"schedule": "month_end", "band": 0.0},
        **changes,
    }


def _plan(trial_id: str = "T0002", **changes: Any) -> dict[str, Any]:
    return {
        "schema_version": "research_trial_plan.v1",
        "trial_id": trial_id,
        "owner_decision": DECISION,
        "hypothesis_card": "test card",
        "from_validation_window": True,
        "data": {"snapshot_id": QLH, "signal_snapshot_id": SIG},
        "primary": "mix60",
        "configs": [_fixed("mix60", 0.6), _fixed("mix50", 0.5)],
        "production_effect": "none",
        "broker_action": "none",
        **changes,
    }


def _write(path: Path, data: dict[str, Any]) -> None:
    path.write_bytes(yaml.safe_dump(data, sort_keys=False).encode("utf-8"))


def test_the_p1_plan_tries_a_fixed_mix_with_the_treasury() -> None:
    gate = tr.load_gate(GATE_FILE)
    plan = tr.load_plan(PLAN_FILE, gate)

    assert (plan.trial_id, plan.owner_decision, plan.candidate) == ("T0002", DECISION, None)
    assert plan.from_validation_window is True
    assert (plan.data.snapshot_id, plan.data.signal_snapshot_id) == (
        "qlh-20261013-0b0df53cb1a2",
        "sig-7ad8aa00b736",
    )
    assert plan.primary == "qqq60_t10y40"
    assert [
        (c.id, c.function, c.params, c.reserve, c.rebalance.schedule, c.rebalance.band)
        for c in plan.configs
    ] == [
        ("qqq60_t10y40", "fixed_exposure", {"exposure": 0.6}, "treasury_10y", "month_end", 0.0),
        ("qqq50_t10y50", "fixed_exposure", {"exposure": 0.5}, "treasury_10y", "month_end", 0.0),
        ("qqq70_t10y30", "fixed_exposure", {"exposure": 0.7}, "treasury_10y", "month_end", 0.0),
    ]
    assert all(c.delay_days == 0 and not c.leveraged_benchmarks for c in plan.configs)
    assert tr.plan_problems(PROJECT_ROOT / "research", plan) == []


def _bad_plans() -> list[tuple[str, dict[str, Any], str]]:
    candidate = {"signals": ["tsmom_21"], "screens": ["S0001"]}
    no_decision = {k: v for k, v in _plan().items() if k != "owner_decision"}
    signal_config = {
        "id": "signal",
        "function": "signal_exposure",
        "params": {"signals": [], "mapping": "threshold", "low": 0, "high": 1},
    }
    return [
        ("both", _plan(candidate=candidate), "not both"),
        ("neither", no_decision, "screened candidate or the owner decision"),
        ("bad decision", _plan(owner_decision="portfolio p1"), "should match pattern"),
        (
            "other function",
            _plan(configs=[_fixed("mix60", 0.6, function="trend")]),
            "fixed_exposure",
        ),
        (
            "signal without candidate",
            _plan(configs=[_fixed("mix60", 0.6), signal_config]),
            "candidate",
        ),
        (
            "extra parameter",
            _plan(configs=[{**_fixed("mix60", 0.6), "params": {"exposure": 0.6, "x": 1}}]),
            "exactly one parameter",
        ),
        (
            "no exposure",
            _plan(configs=[{**_fixed("mix60", 0.6), "params": {}}]),
            "exactly one parameter",
        ),
        ("bool exposure", _plan(configs=[_fixed("mix60", True)]), "must be numbers"),
        (
            "text exposure",
            _plan(configs=[{**_fixed("mix60", 0.6), "params": {"exposure": "0.6"}}]),
            "must be numbers",
        ),
        ("above 1", _plan(configs=[_fixed("mix60", 1.2)]), "between 0 and 1"),
        ("below 0", _plan(configs=[_fixed("mix60", -0.1)]), "between 0 and 1"),
        ("unknown reserve", _plan(configs=[_fixed("mix60", 0.6, reserve="gold")]), "reserve"),
        ("delay", _plan(configs=[_fixed("mix60", 0.6, delay_days=1)]), "no delay"),
    ]


@pytest.mark.parametrize(
    ("name", "plan", "match"), _bad_plans(), ids=[n for n, _, _ in _bad_plans()]
)
def test_decision_plan_refusals(
    tmp_path: Path, name: str, plan: dict[str, Any], match: str
) -> None:
    path = tmp_path.joinpath(f"{plan['trial_id']}.yaml")
    _write(path, plan)
    with pytest.raises(ValueError, match=match):
        tr.load_plan(path)


def test_a_config_holds_cash_unless_it_names_a_reserve(tmp_path: Path) -> None:
    config = _fixed("mix60", 0.6)
    del config["reserve"]
    path = tmp_path.joinpath("T0002.yaml")
    _write(path, _plan(configs=[config]))
    assert tr.load_plan(path).config("mix60").reserve == "cash"


def test_market_data_adds_the_treasury_from_the_signal_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = tr.load_gate(GATE_FILE)
    plan = tr.TrialPlan.model_validate(_plan())
    factors = _factors("1990-01-01", "1990-03-30")
    yields = _yields(pd.bdate_range("1989-12-01", "2011-06-30", name="date"))
    specs = tuple(
        SourceSpec(
            source_id=source,
            provider=provider,
            dataset=dataset,
            frequency="daily",
            known_lag_trading_days=lag,
            description=source,
        )
        for source, provider, dataset, lag in (
            ("ff_factors_daily", "ken_french", "F-F_Research_Data_Factors_daily", 0),
            ("dgs10", "fred", "DGS10", 1),
        )
    )
    snapshot = SignalSnapshot(SIG, {"ff_factors_daily": factors, "dgs10": yields}, specs)
    monkeypatch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
    monkeypatch.setattr(tr, "from_snapshot", lambda directory, *, qqq_series: _qqq_data())
    monkeypatch.setattr(qlh, "resolve_snapshot_dir", lambda root, snapshot_id: root / snapshot_id)

    for market in ("us_market", "qqq"):
        data = tr.market_data(market, gate, plan, snapshot_root=tmp_path, project_root=PROJECT_ROOT)
        calendar = pd.DatetimeIndex(data.returns.index)
        # The return uses the yield as of each date (no lag); the signal column keeps its lag.
        pd.testing.assert_series_equal(
            data.reserves[TREASURY_RESERVE],
            treasury_returns(yields["value"], calendar),
            check_names=False,
        )
        assert data.reserves[TREASURY_RESERVE].iloc[1:].notna().all()
        assert "source:dgs10:value" in data.inputs.columns


# Trials with a reserve --------------------------------------------------------------------------


def _git(cwd: Path, *args: str) -> str:
    done = subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)
    return done.stdout.strip()


def _repo(
    base: Path, *, plan: dict[str, Any] | None = None, decision: bool = True, more: int = 0
) -> Path:
    """A repository with the real gate, plan T0002 and both decisions on main (no screening
    ledger); returns its research root."""

    top = base.joinpath("repo")
    trials = top.joinpath("research", "_trials")
    trials.mkdir(parents=True)
    top.joinpath("docs", "requirements").mkdir(parents=True)
    trials.joinpath("prereg.yaml").write_bytes(GATE_FILE.read_bytes())
    _git(top, "init", "-q", "-b", "main")
    _git(top, "config", "user.email", "test@example.com")
    _git(top, "config", "user.name", "test")
    lines = [f"- {GATE_DECISION}: two-stage ruler with the tail floor."]
    if decision:
        lines.append(f"- {DECISION}: P1.")
    top.joinpath("docs", "requirements", "TRADING-2567.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    _write(trials.joinpath("T0002.yaml"), plan or _plan())
    for number in range(3, 3 + more):
        _write(trials.joinpath(f"T{number:04d}.yaml"), _plan(f"T{number:04d}"))
    _git(top, "add", "-A")
    _git(top, "commit", "-q", "-m", "trial gate and plan")
    return top.joinpath("research")


def _runs(root: Path) -> list[Path]:
    runs = root.joinpath("_trials", "runs")
    return sorted(runs.glob("*.json")) if runs.exists() else []


def test_a_decision_plan_needs_its_decision_on_main_and_no_screen(tmp_path: Path) -> None:
    root = _repo(tmp_path.joinpath("a"))
    _, plan = tr.load_trial(root, "T0002")
    gate = tr.load_gate(root.joinpath("_trials", "prereg.yaml"))
    assert tr.approval_problems(root, plan, gate, "main") == []

    root = _repo(tmp_path.joinpath("b"), decision=False)
    assert tr.approval_problems(root, plan, gate, "main") == [
        f"{DECISION} is not in docs/requirements on main"
    ]
    with pytest.raises(tr.TrialError, match="not approved"):
        tr.run_trial(root, "T0002", "mix60", "qqq", QQQ, loaded=LOADED)
    assert _runs(root) == []


def test_a_second_plan_for_the_same_decision_is_refused(tmp_path: Path) -> None:
    root = _repo(tmp_path, more=1)
    _, plan = tr.load_trial(root, "T0002")
    assert tr.plan_problems(root, plan) == [f"T0003 already tries {DECISION}"]


@pytest.fixture(scope="module")
def trial(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = _repo(tmp_path_factory.mktemp("trial"))
    records = [
        tr.run_trial(root, "T0002", "mix60", "qqq", QQQ, loaded=LOADED),
        tr.run_trial(root, "T0002", "mix60", "us_market", US, loaded=LOADED),
    ]
    return {"root": root, "records": records}


def test_the_us_market_run_with_the_treasury_uses_the_1962_window(trial: dict[str, Any]) -> None:
    record = trial["records"][1]
    spec = RunSpec(
        question_type="us_market_bond_trial",
        strategy_id="mix60",
        params={"exposure": 0.6},
        window_role="development",
        rebalance=MONTHLY,
        reserve="treasury_10y",
    )

    assert record["status"] == "ok"
    assert record["window"] == {
        "window_id": "us_market_1962_2010",
        "role": "development",
        "start": "1962-01-02",
        "end": "2010-12-31",
    }
    assert record["spec"] == spec_record(spec)
    assert record["strategy"]["function"] == "fixed_exposure"
    direct = run_strategy(US, fixed_exposure, spec, LOADED, falsification_confidence=0.95)
    assert record["result"]["fingerprint"] == direct.fingerprint
    assert record["result"]["diagnostics"] == lg._plain(direct.diagnostics)
    # The read-only segment tables use the lists of the 1962 window (slow_bear_readonly).
    slow, long = tr.load_gate(GATE_FILE).markets["us_market"].segment_lists("us_market_1962_2010")
    segments = record["result"]["segments"]
    assert [row["id"] for row in segments["slow_bear"]] == [
        "slow_bear_196811",
        "slow_bear_197212",
        "slow_bear_200003",
        "slow_bear_200710",
    ]
    assert [row["id"] for row in segments["long_bull"]] == [e.id for e in long]
    listed = run_strategy(US, fixed_exposure, spec, LOADED, slow_bears=slow, long_bulls=long)
    assert segments == lg._plain(listed.segments)


def test_the_qqq_run_with_the_treasury_uses_the_qqq_window(trial: dict[str, Any]) -> None:
    record = trial["records"][0]
    spec = RunSpec(
        question_type="qqq_direction",
        strategy_id="mix60",
        params={"exposure": 0.6},
        window_role="development",
        rebalance=MONTHLY,
        reserve="treasury_10y",
    )

    assert record["status"] == "ok"
    assert record["window"]["window_id"] == "qqq_long_history_development"
    assert record["spec"] == spec_record(spec)
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
        "segments",
    }
    direct = run_strategy(QQQ, fixed_exposure, spec, LOADED, falsification_confidence=0.95)
    assert record["result"]["fingerprint"] == direct.fingerprint


def test_a_reserve_the_data_lack_refuses_the_trial(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    plain = with_signal_sources(_qqq_data(), {}, {}, snapshot_id=SIG)
    with pytest.raises(tr.TrialError, match="no treasury_10y reserve"):
        tr.run_trial(root, "T0002", "mix60", "qqq", plain, loaded=LOADED)
    late = with_reserves(
        with_signal_sources(
            from_us_market({"ff_factors_daily": _factors()}, snapshot_id=SIG),
            {},
            {},
            snapshot_id=SIG,
        ),
        {TREASURY_SOURCE: _yields(pd.bdate_range("1970-01-02", "2011-06-30"))},
    )
    with pytest.raises(tr.TrialError, match="treasury_10y has no return on 1962-01-03"):
        tr.run_trial(root, "T0002", "mix60", "us_market", late, loaded=LOADED)
    assert _runs(root) == []


def test_the_report_shows_the_design_the_reserve_and_the_two_lines(trial: dict[str, Any]) -> None:
    lines = tr.trial_report(trial["root"], "T0002", LOADED)
    summary = tr.trial_summary(trial["root"], "T0002", LOADED)

    assert (summary["signals"], summary["screens"], summary["family_size"]) == ([], [], None)
    assert summary["owner_decision"] == DECISION
    assert [run["reserve"] for run in summary["runs"]] == ["treasury_10y", "treasury_10y"]
    assert f"- 设计：没有筛选候选，按 {DECISION} 试跑" in lines
    assert "- 想法来自验证窗口：是" in lines
    assert not any(line.startswith("- 信号：") for line in lines)
    assert "## mix60 · QQQ · 储备 10 年期国债（trial-r0001，ok）" in lines
    assert "## mix60 · 美股大盘 · 储备 10 年期国债（trial-r0002，ok）" in lines
    for record in trial["records"]:
        result = record["result"]
        assert tr.cost_benefit_line(result["comparison"]) in lines
        assert tr.matched_volatility_line(result["diagnostics"]) in lines


def test_cost_benefit_line() -> None:
    def line(g: Any, t: Any = 0.10, d: Any = 0.05) -> str:
        return tr.cost_benefit_line(
            {"geometric_annual_return": g, "cvar_5pct_relative_improvement": t, "max_drawdown": d}
        )

    assert line(-0.02) == "- 代价换效果：每放弃 1 个点几何年化，左尾改善 5.0%，回撤浅 2.5 个点"
    assert line(-0.001, 0.02, 0.01) == (
        "- 代价换效果：每放弃 1 个点几何年化，左尾改善 20.0%，回撤浅 10.0 个点"
    )
    assert line(-0.04, -0.02, -0.01) == (
        "- 代价换效果：每放弃 1 个点几何年化，左尾改善 -0.5%，回撤浅 -0.2 个点"
    )
    assert line(0.003) == "- 代价换效果：没有放弃收益（几何年化 +0.3 个点）"
    assert line(-0.0004) == "- 代价换效果：没有放弃收益（几何年化 -0.0 个点）"
    assert line(-0.0008) == "- 代价换效果：没有放弃收益（几何年化 -0.1 个点）"
    assert line(None) == "- 代价换效果：-"
    assert line(-0.02, math.nan) == "- 代价换效果：-"
    assert tr.cost_benefit_line({}) == "- 代价换效果：-"


def test_matched_volatility_line() -> None:
    diagnostic = {
        "exposure": 0.42,
        "comparison": {
            "geometric_annual_return": 0.004,
            "max_drawdown": -0.012,
            "cvar_5pct_relative_improvement": 0.031,
        },
    }
    assert tr.matched_volatility_line({"matched_volatility": diagnostic}) == (
        "- 同波动率对照（QQQ 加现金，固定敞口 0.42）：几何年化 +0.4 个点，回撤浅 -1.2 个点，"
        "左尾改善 3.1%"
    )
    assert tr.matched_volatility_line(None) == "- 同波动率对照：-"
    assert tr.matched_volatility_line({"matched_volatility": None}) == "- 同波动率对照：-"


def test_reserve_names() -> None:
    # Runs that hold cash (every run before this change) keep the old header.
    assert tr.RESERVE_NAMES == {"cash": "现金", "treasury_10y": "10 年期国债"}
