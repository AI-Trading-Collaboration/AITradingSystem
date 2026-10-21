"""Synthetic edge cases and command wiring for TRADING-2565 S4b; never open real snapshots."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_bench import __main__ as cli
from ai_trading_system.research_bench.ledger import history_problems
from ai_trading_system.research_bench.screen import (
    ScreenCheckError,
    ScreenMarket,
    bh_adjust,
    expanding_percentile,
    load_signal,
    market_statistics,
    minimum_detectable_ic,
    screen_signal,
)
from ai_trading_system.research_bench.screen_ledger import (
    ScreenLedgerError,
    append_screen_record,
    family_key,
    screen_ledger_summary,
    screen_records,
    verify_screen_ledger,
)
from ai_trading_system.research_bench.screen_settings import ScreenRules, load_screen_settings
from ai_trading_system.research_bench.sources import SourceSpec

RULES = load_screen_settings().settings.screen


def _signal(
    tmp_path: Path, body: str = "return returns.rolling(params['days']).sum()", **changes: Any
) -> Any:
    spec = {
        "signal_id": "test_signal",
        "mechanism": "synthetic",
        "expected_sign": 1,
        "target": "return",
        "params": {"days": 3},
        "sources": [],
        "markets": "all",
        "literature": "none",
        **changes,
    }
    (tmp_path / "test_signal.yaml").write_text(json.dumps(spec), encoding="utf-8")
    (tmp_path / "test_signal.py").write_text(
        "def signal(returns, sources, params):\n    " + body + "\n", encoding="utf-8"
    )
    return load_signal("test_signal", tmp_path)


def _markets() -> dict[str, ScreenMarket]:
    dates = pd.bdate_range("2008-01-01", "2011-12-31")
    r = pd.Series(np.random.default_rng(4).normal(0, 0.01, len(dates)), index=dates)
    return {k: ScreenMarket(k, r.copy(), date(2009, 1, 1)) for k in ("qqq", "us_market")}


def _body(**changes: Any) -> dict[str, Any]:
    return {
        "signal_id": "test_signal",
        "module_sha256": "a" * 64,
        "spec_sha256": "b" * 64,
        "params": {},
        "markets": ["qqq", "us_market"],
        "data": {"signal_snapshot_id": "sig-synthetic", "bench_snapshot_id": "qlh-synthetic"},
        "code": {"commit": "c" * 40, "modified": False},
        "settings_sha256": "d" * 64,
        "results": {},
        "primary_p_value": 0.04,
        "verdict": "inconclusive",
        "family_size": 1,
        **changes,
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"screen_end": "2011-01-03"},
        {"horizons": []},
        {"horizons": [21, 21]},
        {"horizons": [0, 21]},
        {"primary_horizon": 2},
        {"bootstrap_markets": ["qqq"]},
        {"bootstrap_markets": ["us_market"]},
        {"eliminate_breadth": 0.9},
        {"bh_q": 1},
        {"copy_corr": 0},
        {"drawdown_threshold": 0},
        {"confidence": 1},
        {"groups": 0},
        {"warmup_days": 0},
        {"min_days": 0},
        {"seed": 0},
        {"bootstrap_draws": 0},
        {"lookahead_cut_dates": 0},
        {"unknown": True},
    ],
)
def test_screen_rules_reject_invalid_values(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        ScreenRules.model_validate(RULES.model_dump() | changes)


def test_load_signal_failures_and_platform_independent_hashes(tmp_path: Path) -> None:
    with pytest.raises(ScreenCheckError):
        load_signal("missing", tmp_path)
    for bad in ("../test_signal", "MixedCase", ""):
        with pytest.raises(ScreenCheckError):
            load_signal(bad, tmp_path)
    loaded = _signal(tmp_path)
    for suffix in ("py", "yaml"):
        path = tmp_path / f"test_signal.{suffix}"
        path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    again = load_signal("test_signal", tmp_path)
    assert (loaded.module_sha256, loaded.spec_sha256) == (again.module_sha256, again.spec_sha256)
    assert (
        loaded.module_sha256
        == hashlib.sha256(
            (tmp_path / "test_signal.py").read_bytes().replace(b"\r\n", b"\n")
        ).hexdigest()
    )
    for changes in (
        {"signal_id": "other"},
        {"mechanism": " "},
        {"literature": " "},
        {"expected_sign": 0},
        {"extra": True},
    ):
        with pytest.raises(ScreenCheckError):
            _signal(tmp_path, **changes)
    _signal(tmp_path)
    (tmp_path / "test_signal.py").write_text("signal = 1", encoding="utf-8")
    with pytest.raises(ScreenCheckError):
        load_signal("test_signal", tmp_path)


@pytest.mark.parametrize(
    "body",
    [
        "return list(returns)",
        "return returns.iloc[1:]",
        "return returns * float('inf')",
        "return returns.astype(str)",
        "return returns.astype(complex)",
    ],
)
def test_invalid_signal_output_is_refused(tmp_path: Path, body: str) -> None:
    loaded = _signal(tmp_path, body, params={})
    with pytest.raises(ScreenCheckError):
        screen_signal(loaded, _markets(), {}, {}, RULES)


def test_parameters_missing_markets_and_qqq_only(tmp_path: Path) -> None:
    with pytest.raises(ScreenCheckError, match="does not change"):
        screen_signal(_signal(tmp_path, "return returns"), _markets(), {}, {}, RULES)
    loaded = _signal(tmp_path)
    with pytest.raises(ScreenCheckError, match="missing market"):
        screen_signal(loaded, {"qqq": _markets()["qqq"]}, {}, {}, RULES)
    only = _signal(tmp_path, markets="qqq", params={"days": 3, "description": "unchecked"})
    result = screen_signal(
        only, {"qqq": _markets()["qqq"]}, {}, {}, RULES.model_copy(update={"bootstrap_draws": 10})
    )
    assert result["unchecked"] == ["description"]
    assert result["breadth"] is result["primary_p_value"] is None
    assert result["verdict_inputs"]["primary_interval"] is None
    assert set(result["markets"]) == {"qqq"}
    json.dumps(result, allow_nan=False)


def test_expanding_percentile_ties_missing_and_past_only() -> None:
    values = pd.Series(np.random.default_rng(1).integers(-3, 4, 100).astype(float))
    values.iloc[::7] = np.nan
    expected = []
    for t, value in enumerate(values):
        past = values.iloc[: t + 1].dropna()
        expected.append(
            ((past < value).sum() + 0.5 * ((past == value).sum() - 1)) / (len(past) - 1)
            if len(past) > 4 and pd.notna(value)
            else np.nan
        )
    np.testing.assert_allclose(expanding_percentile(values, 4), expected)
    with pytest.raises(ValueError):
        expanding_percentile(values, 0)


def test_constant_and_missing_values_are_plain_json() -> None:
    r = _markets()["qqq"].returns
    for signal in (r * 0, r * np.nan):
        stats = market_statistics(
            signal,
            r,
            eval_start=date(2009, 1, 1),
            screen_end=RULES.screen_end,
            settings=RULES,
            expected_sign=1,
            target="return",
            bootstrap=True,
        )
        assert stats["horizons"]["21"]["ic"] is None
        json.dumps(stats, allow_nan=False)
    with pytest.raises(ValueError):
        minimum_detectable_ic(0)
    assert bh_adjust([]) == []
    assert bh_adjust([0, 1, 0]) == [0, 1, 0]
    for bad in ([np.nan], [-0.1], [1.1]):
        with pytest.raises(ValueError):
            bh_adjust(bad)


@pytest.mark.parametrize("key", sorted(_body()))
def test_record_requires_all_body_keys(tmp_path: Path, key: str) -> None:
    body = _body()
    del body[key]
    with pytest.raises(ValueError):
        append_screen_record(tmp_path, body)


@pytest.mark.parametrize(
    "key", ["schema_version", "number", "screen_id", "recorded_at", "prev_sha256"]
)
def test_record_rejects_reserved_keys(tmp_path: Path, key: str) -> None:
    with pytest.raises(ValueError):
        append_screen_record(tmp_path, _body(**{key: None}))


def test_broken_chain_refuses_reads_and_append(tmp_path: Path) -> None:
    directory = tmp_path / "research" / "_screens" / "ledger"
    append_screen_record(tmp_path, _body())
    path = directory / "0001.json"
    record = json.loads(path.read_bytes())
    record["prev_sha256"] = "bad"
    path.write_text(json.dumps(record), encoding="utf-8")
    assert verify_screen_ledger(tmp_path)
    with pytest.raises(ScreenLedgerError):
        screen_records(tmp_path)
    with pytest.raises(ScreenLedgerError):
        append_screen_record(tmp_path, _body(spec_sha256="new"))
    path.write_text("not JSON", encoding="utf-8")
    assert verify_screen_ledger(tmp_path)
    path.write_text("[]", encoding="utf-8")
    assert verify_screen_ledger(tmp_path)
    (directory / "other.txt").write_text("invalid", encoding="utf-8")
    assert any("not a screen record" in p for p in verify_screen_ledger(tmp_path))


def test_family_latest_and_current_verdict(tmp_path: Path) -> None:
    inputs = {
        "expected_sign": 1,
        "qqq_ic": 0.1,
        "qqq_interval": [0, 0.2],
        "primary_interval": [0, 0.2],
        "breadth": 1,
        "leave_out_stable": True,
        "baseline_max_abs_corr": 0.2,
        "qqq_only": False,
    }
    a = _body(primary_p_value=0.08, results={"verdict_inputs": inputs}, verdict="candidate")
    append_screen_record(tmp_path, a)
    assert screen_ledger_summary(tmp_path, RULES)["members"][0]["verdict"] == "candidate"
    append_screen_record(tmp_path, _body(module_sha256="second", primary_p_value=0.8))
    summary = screen_ledger_summary(tmp_path, RULES)
    assert summary["members"][0]["verdict_at_record"] == "candidate"
    assert summary["members"][0]["verdict"] == "inconclusive"
    append_screen_record(
        tmp_path,
        a | {"data": a["data"] | {"signal_snapshot_id": "different"}, "primary_p_value": 0.01},
    )
    append_screen_record(tmp_path, _body(module_sha256="third", primary_p_value=None))
    summary = screen_ledger_summary(tmp_path, RULES)
    assert summary["family_size"] == 3 and summary["tested"] == 2
    assert summary["members"][0]["records"] == 2
    assert summary["members"][0]["adjusted_p"] == pytest.approx(0.02)
    assert summary["members"][2]["adjusted_p"] is None
    assert summary["members"][1]["verdict"] is None
    with pytest.raises(ValueError):
        append_screen_record(tmp_path, _body(verdict="approved"))
    assert family_key(a) == f"{a['module_sha256']}:{a['spec_sha256']}"


def _git(top: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=top, check=True, capture_output=True, text=True)


@pytest.mark.parametrize("action", ["modify", "delete", "rename"])
def test_git_history_refuses_screen_record_changes(tmp_path: Path, action: str) -> None:
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.name", "test")
    _git(tmp_path, "config", "user.email", "test@example.com")
    append_screen_record(tmp_path, _body())
    _git(tmp_path, "add", "research")
    _git(tmp_path, "commit", "-q", "-m", "add screen")
    assert history_problems(tmp_path) == []
    path = tmp_path / "research" / "_screens" / "ledger" / "0001.json"
    if action == "modify":
        path.write_bytes(path.read_bytes().replace(b"0.04", b"0.05"))
    elif action == "delete":
        path.unlink()
    else:
        path.rename(path.with_name("0002.json"))
    _git(tmp_path, "add", "research")
    _git(tmp_path, "commit", "-q", "-m", action)
    assert any("screen record research/_screens/ledger/" in p for p in history_problems(tmp_path))


def _cli_setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    loaded = _signal(tmp_path)
    monkeypatch.setattr(cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(cli, "load_signal", lambda _: loaded)
    monkeypatch.setattr(
        cli.qlh, "resolve_snapshot_dir", lambda root, snapshot: root / (snapshot or "qlh-synthetic")
    )
    return loaded


def test_cli_duplicate_and_broken_chain_refuse_before_loading_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loaded = _cli_setup(tmp_path, monkeypatch)
    body = _body(
        module_sha256=loaded.module_sha256,
        spec_sha256=loaded.spec_sha256,
        settings_sha256=load_screen_settings().sha256,
    )
    append_screen_record(tmp_path, body)

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("snapshot loader must not be called")

    monkeypatch.setattr(cli.signal_sources, "load_snapshot", forbidden)
    monkeypatch.setattr(cli, "from_snapshot", forbidden)
    runner = CliRunner()
    args = [
        "screen",
        "test_signal",
        "--signal-snapshot",
        "sig-synthetic",
        "--bench-snapshot",
        "qlh-synthetic",
    ]
    result = runner.invoke(cli.app, args)
    assert result.exit_code == 1 and "不记账" in result.stdout
    ledger = tmp_path / "research" / "_screens" / "ledger" / "0001.json"
    ledger.write_bytes(b"invalid")
    result = runner.invoke(cli.app, args)
    assert result.exit_code == 1
    result = runner.invoke(cli.app, ["screen-ledger"])
    assert result.exit_code == 1 and "BROKEN" in result.stdout


def test_cli_success_records_synthetic_inputs_and_check_failure_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cli_setup(tmp_path, monkeypatch)
    markets = _markets()
    r = markets["qqq"].returns
    snapshot = SimpleNamespace(
        frames={
            "ff_factors_daily": pd.DataFrame({"Mkt-RF": r, "RF": r * 0}),
            "ff_49_industries_daily": pd.DataFrame({"Food & Drink": r}),
        },
        specs=(),
    )
    monkeypatch.setattr(cli.signal_sources, "load_snapshot", lambda *args: snapshot)
    monkeypatch.setattr(
        cli, "from_snapshot", lambda *args, **kwargs: SimpleNamespace(inputs={"ndx_return": r})
    )
    monkeypatch.setattr(
        cli, "git_provenance", lambda _: {"commit": "c" * 40, "code_modified": False}
    )
    settings = load_screen_settings()
    monkeypatch.setattr(
        cli,
        "load_screen_settings",
        lambda: SimpleNamespace(
            sha256=settings.sha256,
            settings=SimpleNamespace(screen=RULES.model_copy(update={"bootstrap_draws": 10})),
        ),
    )
    runner = CliRunner()
    args = [
        "screen",
        "test_signal",
        "--signal-snapshot",
        "sig-synthetic",
        "--bench-snapshot",
        "qlh-synthetic",
    ]
    result = runner.invoke(cli.app, args)
    assert result.exit_code == 0, result.exception
    assert "S0001" in result.stdout and "家族大小 1" in result.stdout
    record = screen_records(tmp_path)[0]
    assert record["markets"] == ["industry_food___drink", "qqq", "us_market"]
    assert record["code"] == {"commit": "c" * 40, "modified": False}
    assert record["settings_sha256"] == settings.sha256
    assert "逐日" not in result.stdout
    assert runner.invoke(cli.app, ["screen-ledger"]).exit_code == 0

    def failed(*args: Any) -> Any:
        raise ScreenCheckError(["synthetic lookahead"])

    monkeypatch.setattr(cli, "screen_signal", failed)
    args[args.index("sig-synthetic")] = "sig-other"
    result = runner.invoke(cli.app, args)
    assert result.exit_code == 1 and "检查未通过" in result.stdout
    assert len(screen_records(tmp_path)) == 1


def test_cli_screen_aligns_each_source_by_its_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TRADING-2565-37: the screen gets each source's spec, so a monthly source is read by month."""

    _cli_setup(tmp_path, monkeypatch)
    r = _markets()["qqq"].returns
    spec = SourceSpec(
        source_id="shiller_price",
        provider="shiller",
        dataset="P",
        frequency="monthly",
        known_lag_trading_days=0,
        description="price",
    )
    snapshot = SimpleNamespace(
        frames={
            "ff_factors_daily": pd.DataFrame({"Mkt-RF": r, "RF": r * 0}),
            "ff_49_industries_daily": pd.DataFrame({"Food & Drink": r}),
            "shiller_price": pd.DataFrame(
                {"value": [1.0]}, index=pd.DatetimeIndex(["2008-01-01"], name="date")
            ),
        },
        specs=(spec,),
    )
    monkeypatch.setattr(cli.signal_sources, "load_snapshot", lambda *args: snapshot)
    monkeypatch.setattr(
        cli, "from_snapshot", lambda *args, **kwargs: SimpleNamespace(inputs={"ndx_return": r})
    )
    seen: list[dict[str, Any]] = []

    def stop(loaded: Any, markets: Any, sources: Any, lags: Any, settings: Any) -> Any:
        seen.append(dict(lags))
        raise ScreenCheckError(["stopped after the source rules"])

    monkeypatch.setattr(cli, "screen_signal", stop)
    args = ["screen", "test_signal", "--signal-snapshot", "sig-synthetic"]
    result = CliRunner().invoke(cli.app, [*args, "--bench-snapshot", "qlh-synthetic"])
    assert result.exit_code == 1 and "检查未通过" in result.stdout
    assert seen == [{"shiller_price": spec}]
    assert seen[0]["shiller_price"] is spec
    assert screen_records(tmp_path) == []
