from __future__ import annotations

import math
import shutil
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
from ai_trading_system.research_bench.deflated import (
    ReturnMoments,
    deflated_sharpe,
    expected_max_sharpe,
    sample_variance,
)
from ai_trading_system.research_bench.metrics import excess_moments
from ai_trading_system.research_bench.prereg import Prereg, Rule, evaluate_rules

LOADED = load_settings()
MODULE = """
def level(inputs, params):
    return inputs["qqq_return"] * 0.0 + float(params["level"])
"""


def _prereg(question_id: str = "QQQ-U1", **changes: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "schema_version": "research_prereg.v1",
        "question_id": question_id,
        "question": "q",
        "question_type": "qqq_direction",
        "data": {"snapshot_id": "snap-1", "qqq_series": "proxy"},
        "strategies": [{"id": "flat", "function": "level", "params": {"level": 0.5}}],
        "validation": {
            "strategies": ["flat"],
            "rules": {"flat": [{"metric": "comparison.max_drawdown", "op": ">=", "value": 0.0}]},
        },
        "prior_expectation": {"effect": "e", "fails_when": "f", "sources": ["s"]},
        "sample_floor": {"min_return_days": 100},
        "trial_cap": 4,
        "stop_conditions": ["cap"],
        "production_effect": "none",
        "broker_action": "none",
    }
    raw.update(changes)
    return raw


def _question_dir(research: Path, question_id: str = "QQQ-U1", **changes: Any) -> Path:
    directory = research.joinpath(question_id)
    directory.mkdir(parents=True)
    directory.joinpath("prereg.yaml").write_text(
        yaml.safe_dump(_prereg(question_id, **changes)), encoding="utf-8"
    )
    directory.joinpath("strategy.py").write_text(MODULE, encoding="utf-8")
    return directory


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


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"data": {"snapshot_id": "LATEST", "qqq_series": "proxy"}}, "fixed snapshot"),
        (
            {"validation": {"strategies": ["other"], "rules": {"other": []}}},
            "unknown strategies",
        ),
        ({"validation": {"strategies": ["flat"], "rules": {}}}, "exactly the validated"),
        ({"validation": {"strategies": ["flat"], "rules": {"flat": []}}}, "without rules"),
        ({"strategy_module": "../x.py"}, "inside the question directory"),
        ({"question_id": "q1"}, "question_id"),
        ({"owner_decision": "TRADING-2567 approved"}, "owner_decision"),
        ({"trial_cap": 0}, "trial_cap"),
        (
            {
                "strategies": [
                    {"id": "flat", "function": "level"},
                    {"id": "flat", "function": "level"},
                ]
            },
            "unique",
        ),
        ({"strategies": [{"id": "flat", "function": "class"}]}, "identifier"),
    ],
)
def test_prereg_refuses_incomplete_or_inconsistent_files(
    changes: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        Prereg.model_validate(_prereg(**changes))


def test_rules_need_a_known_section_and_hold_only_on_a_number() -> None:
    with pytest.raises(ValidationError, match="section"):
        Rule(metric="max_drawdown", op=">=", value=0.0)
    rules = (
        Rule(metric="comparison.max_drawdown", op=">=", value=0.05),
        Rule(metric="strategy.geometric_annual_return", op=">", value=0.0),
        Rule(metric="comparison.rolling_geometric_return_low", op=">=", value=0.0),
        Rule(metric="comparison.sharpe_vs_cash", op="<", value=1.0),
    )
    result = {
        "comparison": {
            "max_drawdown": 0.07,
            "rolling_geometric_return_low": None,
            "sharpe_vs_cash": math.nan,
        },
        "strategy": {"geometric_annual_return": -0.01},
    }
    assert [item["holds"] for item in evaluate_rules(rules, result)] == [
        True,
        False,
        False,
        False,
    ]


def test_excess_moments_by_hand() -> None:
    moments = excess_moments(pd.Series([0.01, -0.01, 0.02, 0.0]))
    # mean 0.005, sample std sqrt(5e-4 / 3); population m2 1.25e-4, m3 0, m4 2.5625e-8.
    assert moments.sharpe == pytest.approx(0.005 / math.sqrt(5e-4 / 3))
    assert moments.skewness == pytest.approx(0.0, abs=1e-12)
    assert moments.kurtosis == pytest.approx(2.5625e-8 / 1.25e-4**2)
    assert moments.observations == 4
    flat = excess_moments(pd.Series([0.001, 0.001, 0.001]))
    assert math.isnan(flat.sharpe) and math.isnan(flat.kurtosis)


def test_deflation_inputs_are_checked() -> None:
    assert expected_max_sharpe(1, 9.0) == 0.0
    assert sample_variance([0.1]) is None
    assert sample_variance([0.1, 0.3]) == pytest.approx(0.02)
    with pytest.raises(ValueError):
        expected_max_sharpe(0, 0.1)
    with pytest.raises(ValueError):
        deflated_sharpe(ReturnMoments(0.1, 0.0, 3.0, 1), 2, 0.1)


def test_chain_digest_ignores_line_endings_and_trial_key_ignores_the_window() -> None:
    assert lg.chain_digest(b'{"a": 1}\r\n') == lg.chain_digest(b'{"a": 1}\n')
    spec = {"strategy_id": "s", "params": {"x": 1}, "window_role": "development"}
    key = lg.trial_key(spec, 4.0, "abc", "proxy")
    assert lg.trial_key({**spec, "window_role": "validation"}, 4.0, "abc", "proxy") == key
    assert lg.trial_key({**spec, "params": {"x": 2}}, 4.0, "abc", "proxy") != key
    assert lg.trial_key(spec, 15.0, "abc", "proxy") != key
    assert lg.trial_key(spec, 4.0, "abd", "proxy") != key
    assert lg.trial_key(spec, 4.0, "abc", "actual") != key


def test_a_question_must_match_its_directory_and_module(tmp_path: Path) -> None:
    directory = _question_dir(tmp_path, "QQQ-U1")
    directory.rename(tmp_path.joinpath("QQQ-U2"))
    with pytest.raises(ValueError, match="names question QQQ-U1"):
        lg.load_question(tmp_path.joinpath("QQQ-U2"))
    other = _question_dir(tmp_path, "QQQ-U3", strategy_module="missing.py")
    with pytest.raises(FileNotFoundError):
        lg.load_question(other)


def test_runs_must_use_the_preregistered_snapshot(tmp_path: Path) -> None:
    question = lg.load_question(_question_dir(tmp_path))
    with pytest.raises(lg.LedgerError, match="snapshot snap-1"):
        run_and_record(question, _data("snap-2"), "flat", loaded=LOADED)
    assert lg.run_records(question.directory) == []


def test_summary_counts_trials_over_all_questions(tmp_path: Path) -> None:
    first = lg.load_question(_question_dir(tmp_path, "QQQ-U1"))
    second = lg.load_question(_question_dir(tmp_path, "QQQ-U2"))
    data = _data()
    run_and_record(first, data, "flat", loaded=LOADED)
    run_and_record(first, data, "flat", loaded=LOADED)
    run_and_record(first, data, "flat", params={"level": 0.8}, loaded=LOADED)
    run_and_record(second, data, "flat", params={"level": 0.3}, loaded=LOADED)

    summary = lg.ledger_summary(tmp_path)
    assert summary["global_trials"] == 3
    assert summary["trial_sharpe_variance"] is not None
    assert summary["questions"]["QQQ-U1"]["trials"] == 2
    assert summary["questions"]["QQQ-U1"]["trial_cap"] == 4
    runs = summary["questions"]["QQQ-U1"]["runs"]
    assert [run["trial"] for run in runs] == [1, 1, 2]
    assert all(0.0 <= run["deflated_sharpe"] <= 1.0 for run in runs)


def test_cli_runs_records_and_summarizes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _question_dir(tmp_path)
    monkeypatch.setattr(lg, "research_root", lambda loaded: tmp_path)
    monkeypatch.setattr(cli, "_data", lambda item, root: _data())
    runner = CliRunner()

    result = runner.invoke(cli.app, ["run", "QQQ-U1", "--strategy", "flat", "--set", "level=0.7"])
    assert result.exit_code == 0, result.output
    assert result.output.startswith("QQQ-U1-r0001 flat development trial 1:")
    record = lg.run_records(tmp_path.joinpath("QQQ-U1"))[0]
    assert record["spec"]["params"] == {"level": 0.7}

    result = runner.invoke(cli.app, ["ledger", "QQQ-U1"])
    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[0].startswith("global development trials 1;")
    assert lines[1] == "QQQ-U1: trials 1/4; runs 1; reads -; chain ok"
    assert lines[2].startswith("  QQQ-U1-r0001 flat development ok trial 1 sharpe ")

    result = runner.invoke(cli.app, ["validate", "QQQ-U1"])
    assert result.exit_code == 1
    assert "validation: QQQ-U1 is not approved" in result.output

    result = runner.invoke(cli.app, ["run", "QQQ-U1", "--strategy", "flat", "--set", "level"])
    assert result.exit_code == 1 and "KEY=VALUE" in result.output


def test_the_template_is_a_valid_question_once_copied(tmp_path: Path) -> None:
    template = PROJECT_ROOT.joinpath("research", "_template")
    assert template not in lg.question_directories(PROJECT_ROOT.joinpath("research"))
    directory = tmp_path.joinpath("QQQ-Q0")
    shutil.copytree(template, directory)
    prereg = directory.joinpath("prereg.yaml")
    prereg.write_text(
        prereg.read_text(encoding="utf-8").replace("FILL_SNAPSHOT_ID", "snap-1"), encoding="utf-8"
    )
    question = lg.load_question(directory)
    recorded = run_and_record(question, _data(), "example", loaded=LOADED)
    assert recorded.record["status"] == "ok"
    assert recorded.record["result"]["checks"]["parameters_unchecked"] == []
