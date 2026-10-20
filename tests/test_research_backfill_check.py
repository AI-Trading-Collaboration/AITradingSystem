from __future__ import annotations

import json
import math
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings, screen
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData, from_us_market, with_signal_sources
from ai_trading_system.research_bench.prereg import evaluate_rules, load_prereg
from ai_trading_system.research_bench.run import run_strategy
from ai_trading_system.research_bench.signal_strategy import SIGNALS_DIR, signal_exposure
from ai_trading_system.research_probes import backfill_check as bc

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
# Two configurations keep the whole-run tests fast: a monthly one and the one rebalance override.
RUN_CONFIGS = (bc.CONFIGS[0], bc.CONFIGS[5])


def _us_data(start: str = "1926-07-01", end: str = "2010-12-31", seed: int = 5) -> BenchData:
    """Daily market returns from the Ken French data's first day, with volatility that changes
    month by month (as in the gate_power tests)."""

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
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id="sig-test")
    return with_signal_sources(data, {}, {}, snapshot_id="sig-test")


US = _us_data()


@pytest.fixture(scope="module")
def probe_run(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, Any], Path]:
    journal = tmp_path_factory.mktemp("backfill") / bc.JOURNAL_FILE
    result = bc.run_probe(US, LOADED, GATE, ROOT_DIR, configs=RUN_CONFIGS, journal=journal)
    return result, journal


@pytest.fixture(scope="module")
def probe(probe_run: tuple[dict[str, Any], Path]) -> dict[str, Any]:
    return probe_run[0]


# --- the fixed design ----------------------------------------------------------------------------


def test_the_configs_are_the_six_the_requirement_names() -> None:
    assert [(c.id, c.question, c.strategy, c.decides, c.schedule) for c in bc.CONFIGS] == [
        ("fast_slow", "QQQ-Q3", "fast_slow", True, None),
        ("trend4", "QQQ-Q2", "trend4", True, None),
        ("slow_only", "QQQ-Q3", "slow_only", False, None),
        ("rv21", "QQQ-Q4", "rv21", True, None),
        ("fast_slow_weekly", "QQQ-Q3", "fast_slow_weekly", False, None),
        ("rv21_weekly", "QQQ-Q4", "rv21", False, "week_end"),
    ]
    assert len(bc.CONFIGS) <= GATE.max_configs_per_trial
    assert bc.RUN_ID == "TRADING-2567-33"
    assert bc.OWNER_DECISION == "owner_decision:TRADING-2567:2026-10-20:backfill_check"
    assert bc.DATA_TRIAL == "T0004"


@pytest.mark.parametrize("config", bc.CONFIGS, ids=lambda c: c.id)
def test_each_config_is_its_questions_preregistered_strategy(config: bc.Config) -> None:
    source = load_prereg(ROOT_DIR / config.question / "prereg.yaml").strategy(config.strategy)
    entry = bc.source_entry(ROOT_DIR, config)
    assert entry.function == "signal_exposure"
    assert entry.params == source.params
    assert entry.delay_days == 0 and not entry.leveraged_benchmarks
    assert entry.rebalance.band == source.rebalance.band
    assert entry.rebalance.schedule == (config.schedule or source.rebalance.schedule)
    for ref in entry.params["signals"]:
        pinned = screen.load_signal(ref["id"], directory=SIGNALS_DIR)
        assert (pinned.module_sha256, pinned.spec_sha256) == (
            ref["module_sha256"],
            ref["spec_sha256"],
        )


def test_only_rv21_weekly_changes_its_source_and_only_the_schedule() -> None:
    changed = [c.id for c in bc.CONFIGS if c.schedule is not None]
    assert changed == ["rv21_weekly"]
    weekly = bc.source_entry(ROOT_DIR, bc.CONFIGS[5])
    monthly = bc.source_entry(ROOT_DIR, bc.CONFIGS[3])
    assert weekly.model_dump(exclude={"rebalance"}) == monthly.model_dump(exclude={"rebalance"})
    assert (monthly.rebalance.schedule, weekly.rebalance.schedule) == ("month_end", "week_end")


@pytest.mark.parametrize("config", bc.CONFIGS, ids=lambda c: c.id)
def test_the_run_spec_is_a_trial_runs_on_the_us_market(config: bc.Config) -> None:
    entry = bc.source_entry(ROOT_DIR, config)
    spec = bc.run_spec(config, entry, GATE)
    assert spec.question_type == GATE.markets["us_market"].question_type == "us_market_trial"
    assert spec.window_role == "development"
    assert spec.strategy_id == config.id
    assert spec.reserve == "cash" and spec.delay_days == 0 and not spec.leveraged_benchmarks
    assert spec.rebalance == entry.rebalance.to_rebalance()
    assert spec.cost_bps is None


def test_a_strategy_that_is_not_signal_exposure_is_refused(tmp_path: Path) -> None:
    question = tmp_path / "QQQ-Q1"
    question.mkdir()
    question.joinpath("prereg.yaml").write_bytes((ROOT_DIR / "QQQ-Q1" / "prereg.yaml").read_bytes())
    with pytest.raises(ValueError, match="is not signal_exposure"):
        bc.source_entry(tmp_path, bc.Config("q1a", "QQQ-Q1", "q1a", decides=False))


# --- the pre-start check -------------------------------------------------------------------------


def test_the_check_finds_a_target_on_every_window_day() -> None:
    rows = bc.check_targets(US, LOADED, GATE, ROOT_DIR)
    assert [row["config"] for row in rows] == [c.id for c in bc.CONFIGS]
    assert all(row["ok"] for row in rows), rows
    assert all(row["window_start"] == "1929-01-02" and row["missing_days"] == 0 for row in rows)
    assert all(row["first_valid"] < "1929-01-02" for row in rows)


def test_the_check_fails_when_the_data_start_too_late() -> None:
    late = _us_data(start="1928-06-01")
    rows = {row["config"]: row for row in bc.check_targets(late, LOADED, GATE, ROOT_DIR)}
    # 252 trading days of history are needed before the window's first day.
    assert not rows["fast_slow"]["ok"] and rows["fast_slow"]["missing_days"] > 0
    assert not rows["rv21"]["ok"]


def test_the_check_reports_a_config_that_cannot_load() -> None:
    rows = bc.check_targets(
        US, LOADED, GATE, ROOT_DIR, configs=(bc.Config("nope", "QQQ-Q3", "nope", decides=False),)
    )
    assert rows[0]["ok"] is False and "nope" in rows[0]["error"]


# --- the run ----------------------------------------------------------------------------------


def test_the_probe_names_the_live_gate_the_window_and_the_data(probe: dict[str, Any]) -> None:
    assert probe["run_id"] == bc.RUN_ID and probe["owner_decision"] == bc.OWNER_DECISION
    assert probe["gate_sha256"] == lg.chain_digest(PREREG.read_bytes())
    assert probe["confidence"] == GATE.confidence == 0.95
    assert probe["window"] == {
        "window_id": "us_market_1929_2010",
        "start": "1929-01-02",
        "end": "2010-12-31",
    }
    assert probe["market"] == "us_market" and probe["data_trial"] == "T0004"
    assert probe["source"]["market"] == "us_market"
    assert probe["configs"] == [c.id for c in RUN_CONFIGS]
    assert probe["complete"] is True and probe["reference"] is None


def test_each_row_is_what_a_trial_run_gives(probe: dict[str, Any]) -> None:
    market = GATE.markets["us_market"]
    assert [row["rebalance"]["schedule"] for row in probe["rows"]] == ["month_end", "week_end"]
    # Recomputed for the monthly configuration only, to keep the test fast.
    for config, row in zip(RUN_CONFIGS[:1], probe["rows"][:1], strict=True):
        entry = bc.source_entry(ROOT_DIR, config)
        result = run_strategy(
            US,
            signal_exposure,
            bc.run_spec(config, entry, GATE),
            LOADED,
            falsification_confidence=GATE.confidence,
            episodes=market.event_episodes,
        ).to_dict()
        rows = evaluate_rules(market.rules, {**(result.get("falsification") or {}), **result})
        assert row["status"] == "ok" and row["decides"] == config.decides
        assert [r["metric"] for r in row["gate"]] == [r.metric for r in market.rules]
        assert [r["holds"] for r in row["gate"]] == [r["holds"] for r in rows]
        assert [r["observed"] for r in row["gate"]] == pytest.approx([r["observed"] for r in rows])
        assert row["holds"] == sum(r["holds"] for r in rows) and row["rules"] == 5
        assert row["gate_holds"] == all(r["holds"] for r in rows)
        assert row["window"]["window_id"] == "us_market_1929_2010"
        assert set(row["events"]["episodes"]) == {e.id for e in market.event_episodes or ()}
        assert row["events"]["count"] == result["events"]["count"]


def test_the_journal_holds_the_head_then_every_row(probe_run: tuple[dict[str, Any], Path]) -> None:
    result, journal = probe_run
    lines = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()]
    assert lines[0]["run_id"] == bc.RUN_ID and "rows" not in lines[0]
    assert [line["config"] for line in lines[1:]] == [c.id for c in RUN_CONFIGS]
    assert lines[1:] == result["rows"]


def test_a_failing_config_is_a_row_and_the_run_is_incomplete() -> None:
    result = bc.run_probe(
        US, LOADED, GATE, ROOT_DIR, configs=(bc.Config("nope", "QQQ-Q3", "nope", decides=True),)
    )
    assert result["rows"][0]["status"] == "error" and "nope" in result["rows"][0]["error"]
    assert result["complete"] is False


def test_the_probe_writes_nothing_under_the_research_root(tmp_path: Path) -> None:
    before = sorted(str(path) for path in ROOT_DIR.rglob("*") if "__pycache__" not in path.parts)
    bc.run_probe(
        US,
        LOADED,
        GATE,
        ROOT_DIR,
        configs=(bc.Config("nope", "QQQ-Q3", "nope", decides=True),),
        journal=tmp_path / bc.JOURNAL_FILE,
    )
    after = sorted(str(path) for path in ROOT_DIR.rglob("*") if "__pycache__" not in path.parts)
    assert after == before


# --- T0004 beside the configurations ------------------------------------------------------------


def _record(
    run_id: str,
    *,
    config: str = "dispersion",
    market: str = "us_market",
    status: str = "ok",
    trial: str = "T0004",
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "trial_id": trial,
        "config": config,
        "market": market,
        "status": status,
        "gate": [{"metric": "comparison.max_drawdown", "holds": True, "observed": 0.2}],
        "gate_holds": True,
        "result": {
            "comparison": {"max_drawdown": 0.2},
            "events": {
                "count": 3,
                "outcomes": {"protective": {"count": 1}, "false_alarm": {"count": 2}},
                "episodes": {"crash_1987": {"derisked_share": 0.0, "exit_lag_days": None}},
            },
        },
    }


def test_the_reference_is_t0004s_primary_us_record() -> None:
    records = [
        _record("trial-r0001", trial="T0001"),
        _record("trial-r0002", market="qqq"),
        _record("trial-r0003", config="other"),
        _record("trial-r0004", status="error"),
        _record("trial-r0005"),
    ]
    row = bc.reference_row(records, "T0004", "dispersion")
    assert row is not None
    assert row["run_id"] == "trial-r0005" and row["config"] == "T0004 dispersion"
    assert (row["holds"], row["rules"], row["gate_holds"]) == (1, 1, True)
    assert row["events"]["outcomes"] == {"protective": 1, "false_alarm": 2, "neutral": None}
    assert bc.reference_row(records[:4], "T0004", "dispersion") is None


def test_the_reference_rides_in_the_result_and_the_report() -> None:
    result = bc.run_probe(
        US,
        LOADED,
        GATE,
        ROOT_DIR,
        configs=(bc.Config("nope", "QQQ-Q3", "nope", decides=True),),
        records=[_record("trial-r0031")],
        primary="dispersion",
    )
    assert result["reference"]["run_id"] == "trial-r0031"
    text = bc.render_markdown(result)
    assert "T0004 dispersion" in text and "试跑记录 trial-r0031" in text


# --- the report and the command ------------------------------------------------------------------


def test_the_result_is_written_as_json_and_markdown(probe: dict[str, Any], tmp_path: Path) -> None:
    bc.write_result(probe, tmp_path)
    assert json.loads(tmp_path.joinpath(bc.RESULT_JSON).read_text(encoding="utf-8")) == probe
    text = tmp_path.joinpath(bc.RESULT_MD).read_text(encoding="utf-8")
    assert text.startswith("# 回填检验（TRADING-2567-33）")
    for config in RUN_CONFIGS:
        assert f"| {config.id} | {config.question} {config.strategy} |" in text
    assert "| fast_slow | QQQ-Q3 fast_slow | 判 |" in text
    assert "| rv21_weekly | QQQ-Q4 rv21 | 只读 |" in text
    assert "crash_1929" in text and "global_financial_crisis" in text


def test_the_command_refuses_to_overwrite(tmp_path: Path) -> None:
    tmp_path.joinpath(bc.RESULT_JSON).write_text("{}", encoding="utf-8")
    outcome = CliRunner().invoke(bc.app, ["run", "--out", str(tmp_path)])
    assert outcome.exit_code == 1 and "拒绝覆盖" in outcome.output


def test_the_commands_stop_when_the_data_do_not_load(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail(_: Path) -> Any:
        raise ValueError("no snapshot")

    monkeypatch.setattr(bc, "load_data", fail)
    for args in (["check"], ["run", "--out", str(tmp_path)]):
        outcome = CliRunner().invoke(bc.app, args)
        assert outcome.exit_code == 1 and "数据载入失败：no snapshot" in outcome.output
    assert not tmp_path.joinpath(bc.JOURNAL_FILE).exists()


def test_the_run_stops_when_the_check_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    late = _us_data(start="1928-06-01")
    monkeypatch.setattr(bc, "load_data", lambda _: (late, GATE, LOADED, ROOT_DIR, "dispersion"))
    outcome = CliRunner().invoke(bc.app, ["run", "--out", str(tmp_path)])
    assert outcome.exit_code == 1 and "开跑前检查没过" in outcome.output
    assert not tmp_path.joinpath(bc.JOURNAL_FILE).exists()


def test_the_command_runs_the_probe_and_writes_the_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(bc, "load_data", lambda _: (US, GATE, LOADED, ROOT_DIR, "dispersion"))
    seen: dict[str, Any] = {}
    real = bc.run_probe

    def small(*args: Any, **kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return real(*args, **{**kwargs, "configs": (bc.CONFIGS[0],)})

    monkeypatch.setattr(bc, "run_probe", small)
    outcome = CliRunner().invoke(bc.app, ["run", "--out", str(tmp_path)])
    assert outcome.exit_code == 0, outcome.output
    assert "fast_slow 首个有值日" in outcome.output and "完成" in outcome.output
    assert seen["primary"] == "dispersion" and "configs" not in seen
    result = json.loads(tmp_path.joinpath(bc.RESULT_JSON).read_text(encoding="utf-8"))
    assert [row["config"] for row in result["rows"]] == ["fast_slow"]
    assert tmp_path.joinpath(bc.JOURNAL_FILE).exists() and tmp_path.joinpath(bc.RESULT_MD).exists()


def test_the_loaded_data_are_t0004s_us_market(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def market_data(market: str, gate: tr.TrialGate, plan: tr.TrialPlan, **_: Any) -> BenchData:
        seen.update(market=market, plan=plan.trial_id, gate=gate)
        return replace(US, source={**US.source, "snapshot_id": plan.data.signal_snapshot_id})

    monkeypatch.setattr(tr, "market_data", market_data)
    data, gate, _, root_dir, primary = bc.load_data(Path("unused"))
    assert seen["market"] == "us_market" and seen["plan"] == "T0004"
    assert gate == GATE and root_dir == ROOT_DIR
    assert primary == tr.load_trial(ROOT_DIR, "T0004")[1].primary
    assert data.source["snapshot_id"] == "sig-7ad8aa00b736"


def test_the_loader_refuses_other_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tr, "market_data", lambda *_, **__: US)
    with pytest.raises(ValueError, match="not the us_market of sig-7ad8aa00b736"):
        bc.load_data(Path("unused"))
