"""Synthetic-only wiring and refusal tests for the S4d generic strategy."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import __main__ as cli
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import report, screen_ledger
from ai_trading_system.research_bench import signal_strategy as gs
from ai_trading_system.research_bench.data import BenchData, with_signal_sources
from ai_trading_system.research_bench.prereg import load_prereg
from ai_trading_system.research_bench.settings import load_settings

SIGNAL_SNAPSHOT = "sig-0123456789ab"
REF = {"id": "ident", "module_sha256": "a" * 64, "spec_sha256": "b" * 64, "score": "sign"}


def _question(root: Path, **changes: Any) -> lg.Question:
    raw = load_prereg(PROJECT_ROOT / "research/QQQ-Q1/prereg.yaml").model_dump(mode="json")
    raw.update(
        question_id="QQQ-U1",
        data={"snapshot_id": "bench-1", "qqq_series": "proxy"},
        strategies=[
            {
                "id": "generic",
                "function": "signal_exposure",
                "params": {"signals": [REF], "mapping": "threshold", "low": 0, "high": 1},
            }
        ],
        validation={
            "strategies": ["generic"],
            "rules": {"generic": [{"metric": "comparison.max_drawdown", "op": ">", "value": 0}]},
        },
    )
    raw.update(changes)
    directory = root / "research/QQQ-U1"
    directory.mkdir(parents=True)
    (directory / "prereg.yaml").write_text(yaml.safe_dump(raw), encoding="utf-8")
    (directory / "strategy.py").write_text("# not executed by wiring tests\n", encoding="utf-8")
    return lg.load_question(directory)


def _data() -> BenchData:
    index = pd.bdate_range("2000-01-03", periods=4, name="date")
    return BenchData(
        returns=pd.DataFrame({"qqq": [0.0, 0.01, -0.02, 0.03]}, index=index),
        inputs=pd.DataFrame({"ndx_return": [0.0, 0.01, -0.02, 0.03]}, index=index),
        source={"snapshot_id": "bench-1", "qqq_series": "proxy"},
    )


@pytest.mark.parametrize("actual", [None, "sig-ffffffffffff"])
def test_check_data_refuses_missing_or_wrong_signal_snapshot(
    tmp_path: Path, actual: str | None
) -> None:
    question = _question(
        tmp_path,
        data={
            "snapshot_id": "bench-1",
            "qqq_series": "proxy",
            "signal_snapshot_id": SIGNAL_SNAPSHOT,
        },
    )
    data = _data()
    if actual is not None:
        data = replace(data, source={**data.source, "signal_snapshot_id": actual})
    with pytest.raises(lg.LedgerError, match="signal snapshot"):
        lg._check_data(question, data)
    lg._check_data(
        question, replace(data, source={**data.source, "signal_snapshot_id": SIGNAL_SNAPSHOT})
    )


@pytest.mark.parametrize("actual", [None, SIGNAL_SNAPSHOT])
def test_check_data_refuses_an_undeclared_signal_snapshot(
    tmp_path: Path, actual: str | None
) -> None:
    question = _question(tmp_path)
    data = _data()
    with pytest.raises(lg.LedgerError, match="no preregistered signal snapshot"):
        lg._check_data(
            question, replace(data, source={**data.source, "signal_snapshot_id": actual})
        )
    lg._check_data(question, data)


def _screen_record(root: Path, *, verdict: str = "eliminated", **changes: Any) -> dict[str, Any]:
    body = {
        "signal_id": REF["id"],
        "module_sha256": REF["module_sha256"],
        "spec_sha256": REF["spec_sha256"],
        "params": {},
        "markets": ["qqq"],
        "data": {"signal_snapshot_id": SIGNAL_SNAPSHOT, "bench_snapshot_id": "bench-1"},
        "code": {"commit": "synthetic", "modified": False},
        "settings_sha256": "c" * 64,
        "results": {},
        "primary_p_value": None,
        "verdict": verdict,
        "family_size": 1,
    }
    return screen_ledger.append_screen_record(root, {**body, **changes})


def test_approval_checks_screen_ids_and_the_chain_not_candidate_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    question = _question(tmp_path, screens=["S0001", "S0002"])
    _screen_record(tmp_path)

    def git_output(root: Path, *args: str) -> str:
        return str(tmp_path) if args == ("rev-parse", "--show-toplevel") else "same"

    monkeypatch.setattr(lg, "git_output", git_output)
    assert lg.approval_problems(question, "main") == ["screen S0002 is not in the screening ledger"]
    prereg = question.prereg.model_copy(update={"screens": ("S0001",)})
    assert lg.approval_problems(replace(question, prereg=prereg), "main") == []
    path = tmp_path / "research/_screens/ledger/0001.json"
    path.write_text(
        path.read_text(encoding="utf-8").replace('"prev_sha256": null', '"prev_sha256": "bad"'),
        encoding="utf-8",
    )
    assert any("screening ledger is broken" in p for p in lg.approval_problems(question, "main"))


@pytest.mark.parametrize("matching", [False, True])
def test_report_shows_exact_hash_screening_or_unscreened(tmp_path: Path, matching: bool) -> None:
    _question(tmp_path, screens=["S0001"], screen_family_size=7)
    _screen_record(tmp_path, **({} if matching else {"spec_sha256": "d" * 64}))
    lines = report.question_report(tmp_path / "research", "QQQ-U1", load_settings().settings)
    text = "\n".join(lines)
    assert "screens S0001" in text and "screen_family_size 7" in text
    assert "信号 ident；分数 sign" in text
    assert ("S0001 记录时裁决 eliminated" in text) == matching
    assert ("未筛" in text) != matching
    assert lines[-1] == "no successful run yet"


def test_report_empty_screen_fields_and_broken_screen_chain(tmp_path: Path) -> None:
    _question(tmp_path)
    lines = report.question_report(tmp_path / "research", "QQQ-U1", load_settings().settings)
    assert "筛选：screens 无；screen_family_size 无" in lines
    _screen_record(tmp_path)
    (tmp_path / "research/_screens/ledger/0001.json").write_text("{}", encoding="utf-8")
    with pytest.raises(screen_ledger.ScreenLedgerError):
        report.question_report(tmp_path / "research", "QQQ-U1", load_settings().settings)


def test_cli_data_without_signal_snapshot_is_the_original_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    question, data = _question(tmp_path), _data()
    seen = []

    def from_snapshot(path: Path, *, qqq_series: str) -> BenchData:
        seen.append((path, qqq_series))
        return data

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("no signal snapshot means no extra loader or source columns")

    monkeypatch.setattr(
        cli.qlh, "resolve_snapshot_dir", lambda root, snapshot_id: root / snapshot_id
    )
    monkeypatch.setattr(cli, "from_snapshot", from_snapshot)
    monkeypatch.setattr(cli.signal_sources, "load_snapshot", forbidden)
    monkeypatch.setattr(cli, "with_signal_sources", forbidden)
    got = cli._data(question, tmp_path)
    assert got is data
    assert seen == [(tmp_path / "bench-1", "proxy")]
    assert "signal_snapshot_id" not in got.source


def test_cli_data_uses_the_snapshot_recorded_lag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    question = _question(
        tmp_path,
        data={
            "snapshot_id": "bench-1",
            "qqq_series": "proxy",
            "signal_snapshot_id": SIGNAL_SNAPSHOT,
        },
    )
    data = _data()
    frame = pd.DataFrame({"value": [1.0, 2.0, 3.0, 4.0]}, index=data.inputs.index)
    seen = []

    def load_snapshot(root: Path, snapshot_id: str) -> SimpleNamespace:
        seen.append((root, snapshot_id))
        return SimpleNamespace(
            frames={"spread": frame},
            specs=[
                SimpleNamespace(source_id="spread", known_lag_trading_days=2),
            ],
        )

    monkeypatch.setattr(
        cli.qlh, "resolve_snapshot_dir", lambda root, snapshot_id: root / snapshot_id
    )
    monkeypatch.setattr(cli, "from_snapshot", lambda *args, **kwargs: data)
    monkeypatch.setattr(cli.signal_sources, "load_snapshot", load_snapshot)
    got = cli._data(question, tmp_path)
    expected = with_signal_sources(
        data, {"spread": frame}, {"spread": 2}, snapshot_id=SIGNAL_SNAPSHOT
    )
    pd.testing.assert_frame_equal(got.inputs, expected.inputs)
    assert got.returns is data.returns and got.source == expected.source
    assert seen == [(PROJECT_ROOT, SIGNAL_SNAPSHOT)]


@pytest.mark.parametrize(
    "signals",
    [
        None,
        (),
        [],
        [REF, REF],
        [REF | {"extra": 1}],
        [{"id": "ident"}],
        [REF | {"score": "raw"}],
        [REF | {"id": 1}],
        [REF | {"module_sha256": 1}],
    ],
)
def test_signal_parameters_are_exact(signals: Any) -> None:
    with pytest.raises(ValueError):
        gs.signal_exposure(
            _data().inputs,
            {
                "signals": signals,
                "mapping": "threshold",
                "low": 0,
                "high": 1,
            },
        )


@pytest.mark.parametrize("bad_output", [True, False])
def test_signal_output_must_be_an_aligned_series(
    monkeypatch: pytest.MonkeyPatch,
    bad_output: bool,
) -> None:
    data = _data()

    def signal(returns: pd.Series, sources: Any, params: Any) -> Any:
        return returns.to_numpy() if bad_output else returns.iloc[1:]

    loaded = SimpleNamespace(
        module_sha256=REF["module_sha256"],
        spec_sha256=REF["spec_sha256"],
        spec=SimpleNamespace(sources=(), params={}),
        function=signal,
    )
    monkeypatch.setattr(gs.screen, "load_signal", lambda *args, **kwargs: loaded)
    with pytest.raises(ValueError, match="Series on inputs.index"):
        gs.signal_exposure(
            data.inputs,
            {
                "signals": [REF],
                "mapping": "linear",
                "low": 0,
                "high": 1,
            },
        )


def test_only_declared_sources_and_spec_parameters_are_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = _data()
    data.inputs["source:spread:value"] = 1.0
    data.inputs["source:undeclared:value"] = 99.0
    spec_params = {"scale": 2}

    def signal(returns: pd.Series, sources: Any, params: Any) -> pd.Series:
        assert set(sources) == {"spread"}
        assert sources["spread"].columns.tolist() == ["value"]
        assert sources["spread"].index.equals(data.inputs.index)
        assert params == spec_params and params is not spec_params
        return returns.copy()

    loaded = SimpleNamespace(
        module_sha256=REF["module_sha256"],
        spec_sha256=REF["spec_sha256"],
        spec=SimpleNamespace(
            sources=("spread",), params=spec_params, expected_sign=1, target="return"
        ),
        function=signal,
    )
    monkeypatch.setattr(gs.screen, "load_signal", lambda *args, **kwargs: loaded)
    # No exposure or score bound in this layer, including a cut above one.
    got = gs.signal_exposure(
        data.inputs,
        {
            "signals": [REF],
            "mapping": "threshold",
            "low": 5,
            "high": 6,
            "cut": 1.5,
        },
    )
    assert got.tolist() == [5.0] * len(data.inputs)
    with pytest.raises(ValueError, match="spread.*set data.signal_snapshot_id"):
        gs.signal_exposure(
            data.inputs.drop(columns=["source:spread:value"]),
            {
                "signals": [REF],
                "mapping": "linear",
                "low": 0,
                "high": 1,
            },
        )


@pytest.mark.parametrize("duplicate_existing", [False, True])
def test_source_columns_cannot_collide(duplicate_existing: bool) -> None:
    data = _data()
    frame = pd.DataFrame([[1, 2]] * 4, index=data.inputs.index, columns=["value", "value"])
    if duplicate_existing:
        data.inputs["source:spread:value"] = 1.0
    with pytest.raises(ValueError, match="duplicate signal source column"):
        with_signal_sources(data, {"spread": frame}, {"spread": 1}, snapshot_id=SIGNAL_SNAPSHOT)
