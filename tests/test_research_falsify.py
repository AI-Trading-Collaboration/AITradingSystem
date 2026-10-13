from __future__ import annotations

import math
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml
from pydantic import ValidationError
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import __main__ as cli
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings, run_and_record
from ai_trading_system.research_bench.data import BenchData, from_daily_frame
from ai_trading_system.research_bench.falsify import (
    PAIRED_SECTIONS,
    FalsificationInputs,
    cost_section,
    falsify,
    paired_comparison,
    paired_fields,
    placebo_targets,
    stationary_bootstrap_indices,
)
from ai_trading_system.research_bench.metrics import (
    EXPOSURE_GAP_FIELDS,
    compare,
    comparison_fields,
    exposure_gap,
    summarize,
    summary_fields,
)
from ai_trading_system.research_bench.prereg import (
    Prereg,
    Rule,
    load_prereg,
    rule_problems,
    rule_sections,
)
from ai_trading_system.research_bench.simulate import (
    Rebalance,
    constant_targets,
    schedule_mask,
    simulate,
)

FULL = load_settings()
LOADED = replace(
    FULL,
    settings=FULL.settings.model_copy(
        update={
            "falsification": FULL.settings.falsification.model_copy(
                update={"placebo_draws": 12, "bootstrap_draws": 300}
            )
        }
    ),
)
START, END = date(1999, 3, 10), date(2010, 12, 31)
MODULE = """
def level(inputs, params):
    return inputs["qqq_return"] * 0.0 + float(params["level"])


def momentum(inputs, params):
    trend = inputs["qqq_return"].rolling(int(params["days"])).mean()
    return (trend > 0).astype(float)
"""


def _data(snapshot: str = "snap-1") -> BenchData:
    index = pd.bdate_range("1998-01-02", "2011-06-30", name="date")
    r = np.random.default_rng(5).normal(0.0004, 0.015, len(index))
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "cash_return": 0.0001,
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": 1000.0 * np.cumprod(1 + r),
            "ndx_tr": 1000.0 * np.cumprod(1 + r),
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=index,
    )
    daily.iloc[0, :4] = np.nan
    return from_daily_frame(daily, qqq_series="proxy", source={"snapshot_id": snapshot})


DATA = _data()


def _inputs(targets: pd.Series, rebalance: Rebalance | None = None) -> FalsificationInputs:
    rebalance = rebalance or Rebalance()
    sim = simulate(DATA.returns, targets, start=START, end=END, rebalance=rebalance, cost_bps=4.0)
    matched = simulate(
        DATA.returns,
        constant_targets(DATA.returns.index, float(sim.exposure.mean())),
        start=START,
        end=END,
        rebalance=rebalance,
        cost_bps=4.0,
    )
    return FalsificationInputs(DATA.returns, targets, START, END, rebalance, 0, 4.0, sim, matched)


def _momentum() -> pd.Series:
    return (DATA.inputs["qqq_return"].rolling(10).mean() > 0).astype(float)


def test_bootstrap_paths_are_wrapped_blocks_of_the_right_mean_length() -> None:
    rng = np.random.default_rng(1)
    idx = stationary_bootstrap_indices(500, 200, 10.0, rng)
    assert idx.shape == (200, 500)
    assert idx.min() >= 0 and idx.max() < 500
    steps = (idx[:, 1:] - idx[:, :-1]) % 500
    breaks = steps != 1
    # Blocks end with probability 1/10 (a new block can also start at the next day by chance).
    assert breaks.mean() == pytest.approx(0.1 * (1 - 1 / 500), abs=0.01)
    same = stationary_bootstrap_indices(500, 200, 10.0, np.random.default_rng(1))
    assert np.array_equal(idx, same)
    single = stationary_bootstrap_indices(50, 3, 1.0, rng)
    assert single.shape == (3, 50)
    with pytest.raises(ValueError):
        stationary_bootstrap_indices(50, 3, 0.5, rng)


def test_paired_statistics_agree_with_the_bench_metrics() -> None:
    sim = simulate(
        DATA.returns, _momentum(), start=START, end=END, rebalance=Rebalance(), cost_bps=4.0
    )
    matched = simulate(
        DATA.returns,
        constant_targets(DATA.returns.index, float(sim.exposure.mean())),
        start=START,
        end=END,
        rebalance=Rebalance(),
        cost_bps=4.0,
    )
    cash = DATA.returns["cash"]
    expected = compare(summarize(sim, cash, LOADED), summarize(matched, cash, LOADED), LOADED)
    dates = sim.returns.index
    s = sim.returns.to_numpy()
    b = matched.returns.to_numpy()
    c = cash.reindex(dates).to_numpy()
    paired = paired_comparison(s, b, c, LOADED)
    assert set(paired) == set(paired_fields(LOADED.settings))
    for field in paired:
        if field != "geometric_annual_return":
            assert paired[field] == pytest.approx(expected[field], rel=1e-9, abs=1e-12), field
    # Geometric return is annualized by trading days here, by calendar years in compare.
    by_days = math.expm1(np.log1p(s).mean() * 252) - math.expm1(np.log1p(b).mean() * 252)
    assert paired["geometric_annual_return"] == pytest.approx(by_days)
    stacked = paired_comparison(np.vstack([s, b]), b, c, LOADED)
    for field in paired:
        assert stacked[field][0] == pytest.approx(paired[field], nan_ok=True)
        assert stacked[field][1] == pytest.approx(0.0, abs=1e-15)


def test_the_falsification_has_every_rule_section_with_its_fields() -> None:
    result = falsify(_inputs(_momentum()), LOADED, 0.9)
    sections = rule_sections(LOADED.settings)
    for section, fields in sections.items():
        if section in ("strategy", "comparison", "exposure_gap"):
            continue
        assert set(result[section]) == set(fields), section
    assert set(result) - set(sections) == {"settings", "leave_year", "leave_episode"}
    assert result["settings"]["confidence"] == 0.9
    assert set(result["leave_year"]) == {str(year) for year in range(1999, 2011)}
    assert set(result["leave_episode"]) == {"dotcom_bear", "global_financial_crisis"}
    # The rerun at the unified cost is the main comparison.
    cost = cost_section(LOADED.cost_bps)
    assert cost in result


def test_rule_sections_name_every_field_a_run_produces() -> None:
    sim = simulate(
        DATA.returns, _momentum(), start=START, end=END, rebalance=Rebalance(), cost_bps=4.0
    )
    cash = DATA.returns["cash"]
    summary = summarize(sim, cash, LOADED)
    numeric = {key for key, value in summary.items() if isinstance(value, int | float)}
    assert numeric == set(summary_fields(LOADED.settings))
    assert set(compare(summary, summary, LOADED)) == set(comparison_fields(LOADED.settings))
    assert set(exposure_gap(sim, 0.5, DATA.returns["qqq"])) == set(EXPOSURE_GAP_FIELDS)


def test_placebo_shuffles_the_acted_targets_and_tolerates_unread_gaps() -> None:
    raw = _momentum()
    month_end = Rebalance(schedule="month_end")
    # A monthly strategy may leave the days it never acts on undefined.
    scheduled = np.asarray(schedule_mask(pd.DatetimeIndex(raw.index), "month_end"))
    targets = raw.where(scheduled)
    inputs = _inputs(targets, month_end)
    paths = list(placebo_targets(inputs, LOADED))
    assert len(paths) == LOADED.settings.falsification.placebo_draws
    for path in paths:
        sim = simulate(DATA.returns, path, start=START, end=END, rebalance=month_end, cost_bps=4.0)
        assert len(sim.returns) == len(inputs.strategy.returns)
    assert any(not path.equals(paths[0]) for path in paths[1:])


def test_rank_counts_ties_half_and_constant_strategies_sit_in_the_middle() -> None:
    result = falsify(_inputs(constant_targets(DATA.returns.index, 0.6)), LOADED, 0.95)
    assert set(result["placebo_rank"].values()) <= {0.5, None}
    assert result["paired"]["max_drawdown"] == pytest.approx(0.0, abs=1e-15)


@pytest.mark.parametrize(
    ("metric", "ok"),
    [
        ("ci_low.max_drawdown", True),
        ("placebo_rank.geometric_annual_return", True),
        ("leave_episode_min.cvar_5pct_relative_improvement", True),
        ("cost_25.max_drawdown", True),
        ("delay_2.annual_turnover", True),
        ("cost_7p5.max_drawdown", True),
        ("leave_year.max_drawdown", False),
        ("cost_x.max_drawdown", False),
        ("ci.max_drawdown", False),
        ("paired", False),
    ],
)
def test_rule_metrics_name_a_section_and_a_field(metric: str, ok: bool) -> None:
    if ok:
        Rule(metric=metric, op=">=", value=0.0)
    else:
        with pytest.raises(ValidationError, match="section"):
            Rule(metric=metric, op=">=", value=0.0)


def _prereg(rules: list[dict[str, Any]], **validation: Any) -> dict[str, Any]:
    return {
        "schema_version": "research_prereg.v1",
        "question_id": "QQQ-U1",
        "question": "Does a short trend filter cut drawdown at a matched exposure?",
        "question_type": "qqq_direction",
        "data": {"snapshot_id": "snap-1", "qqq_series": "proxy"},
        "strategies": [
            {"id": "flat", "function": "level", "params": {"level": 0.5}},
            {"id": "trend", "function": "momentum", "params": {"days": 10}},
        ],
        "validation": {"strategies": ["trend"], "rules": {"trend": rules}, **validation},
        "prior_expectation": {"effect": "e", "fails_when": "f", "sources": ["s"]},
        "sample_floor": {"min_return_days": 100},
        "trial_cap": 4,
        "stop_conditions": ["cap"],
        "production_effect": "none",
        "broker_action": "none",
    }


def test_rules_that_no_run_can_meet_are_found_before_a_read() -> None:
    good = [
        {"metric": "ci_low.max_drawdown", "op": ">", "value": 0.0},
        {"metric": "cost_25.geometric_annual_return", "op": ">=", "value": -0.01},
        {"metric": "strategy.excess_kurtosis", "op": "<", "value": 30.0},
    ]
    assert rule_problems(Prereg.model_validate(_prereg(good)), LOADED.settings) == []
    bad = [
        {"metric": "cost_30.max_drawdown", "op": ">=", "value": 0.0},
        {"metric": "comparison.maxdrawdown", "op": ">=", "value": 0.0},
        {"metric": "strategy.episodes", "op": ">=", "value": 0.0},
        {"metric": "delay_3.max_drawdown", "op": ">=", "value": 0.0},
    ]
    problems = rule_problems(Prereg.model_validate(_prereg(bad)), LOADED.settings)
    assert problems == [f"trend: no metric {rule['metric']}" for rule in bad]
    with pytest.raises(ValidationError, match="confidence"):
        Prereg.model_validate(_prereg(good, confidence=1.0))


def _question(tmp_path: Path, rules: list[dict[str, Any]], **validation: Any) -> lg.Question:
    directory = tmp_path.joinpath("QQQ-U1")
    directory.mkdir()
    directory.joinpath("prereg.yaml").write_text(
        yaml.safe_dump(_prereg(rules, **validation)), encoding="utf-8"
    )
    directory.joinpath("strategy.py").write_text(MODULE, encoding="utf-8")
    return lg.load_question(directory)


RULES = [{"metric": "ci_low.max_drawdown", "op": ">", "value": 0.0}]


def test_development_runs_are_falsified_on_request_without_a_new_trial(tmp_path: Path) -> None:
    question = _question(tmp_path, RULES, confidence=0.99)
    plain = run_and_record(question, DATA, "trend", loaded=LOADED).record
    assert plain["result"]["falsification"] is None
    assert plain["falsification_confidence"] is None
    falsified = run_and_record(question, DATA, "trend", loaded=LOADED, falsify=True).record
    assert falsified["trial"] == {**plain["trial"], "new": False}
    assert falsified["falsification_confidence"] == 0.99
    assert falsified["result"]["falsification"]["settings"]["confidence"] == 0.99
    assert falsified["result"]["fingerprint"] == plain["result"]["fingerprint"]


def test_cli_report_shows_the_first_screen(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _question(tmp_path, RULES)
    monkeypatch.setattr(lg, "research_root", lambda loaded: tmp_path)
    monkeypatch.setattr(cli, "_data", lambda item, root: DATA)
    monkeypatch.setattr(cli, "load_settings", lambda: LOADED)
    runner = CliRunner()
    result = runner.invoke(cli.app, ["run", "QQQ-U1", "--strategy", "flat"])
    assert result.exit_code == 0, result.output
    result = runner.invoke(cli.app, ["run", "QQQ-U1", "--strategy", "trend", "--falsify"])
    assert result.exit_code == 0, result.output
    assert "  falsification: maxDD CI " in result.output

    result = runner.invoke(cli.app, ["report", "QQQ-U1"])
    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[0] == "QQQ-U1: Does a short trend filter cut drawdown at a matched exposure?"
    assert lines[2].startswith("trials 2/4 here, 2 over all questions; reads -; chain ok")
    assert any(line.startswith("QQQ-U1-r0001 flat development") for line in lines)
    assert "  vs matched constant exposure (no falsification; run with --falsify):" in lines
    trend = lines.index(next(line for line in lines if line.startswith("QQQ-U1-r0002 trend")))
    assert lines[trend + 1].startswith("  deflated Sharpe ")
    assert lines[trend + 2].split() == [
        "vs",
        "matched",
        "constant",
        "exposure",
        "diff",
        "CI",
        "low",
        "CI",
        "high",
        "year",
        "worst",
        "episode",
        "worst",
        "placebo",
    ]
    assert lines[trend + 3].split()[0] == "max_drawdown"
    assert any(line.startswith("  cost reruns vs matched: 4 bps: maxDD ") for line in lines)
    assert any(line.startswith("  delay reruns vs matched: +1d: maxDD ") for line in lines)

    result = runner.invoke(cli.app, ["report", "QQQ-U1", "--run", "QQQ-U1-r0002"])
    assert result.exit_code == 0 and "QQQ-U1-r0001" not in result.output
    result = runner.invoke(cli.app, ["report", "QQQ-U1", "--run", "QQQ-U1-r0009"])
    assert result.exit_code == 1 and "no successful run QQQ-U1-r0009" in result.output
    result = runner.invoke(cli.app, ["report", "QQQ-U9"])
    assert result.exit_code == 1 and "no question QQQ-U9" in result.output


def test_paired_sections_are_the_documented_ones() -> None:
    assert PAIRED_SECTIONS == (
        "paired",
        "ci_low",
        "ci_high",
        "leave_year_min",
        "leave_year_max",
        "leave_episode_min",
        "leave_episode_max",
        "placebo_rank",
    )
    assert cost_section(4.0) == "cost_4" and cost_section(7.5) == "cost_7p5"


def test_the_template_rules_are_all_metrics_a_run_has() -> None:
    template = load_prereg(PROJECT_ROOT.joinpath("research", "_template", "prereg.yaml"))
    assert rule_problems(template, FULL.settings) == []
    assert template.validation.confidence == FULL.settings.falsification.confidence
