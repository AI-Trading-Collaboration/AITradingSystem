"""Cases the S4d review found untested (TRADING-2565-10): duplicate signal ids, equal step edges,
an empty linear percentile range, exact source prefixes and the module hash in the report.
Synthetic data only."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import report, screen_ledger
from ai_trading_system.research_bench import signal_strategy as gs
from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.research_bench.prereg import load_prereg
from ai_trading_system.research_bench.settings import load_settings

REF = {"id": "ident", "module_sha256": "a" * 64, "spec_sha256": "b" * 64, "score": "sign"}
INDEX = pd.bdate_range("2004-01-05", periods=30)
INPUTS = pd.DataFrame({"ndx_return": np.linspace(-0.01, 0.01, 30)}, index=INDEX)


def _accepted(ref: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"signals": [ref], "mapping": "threshold", "low": 0.0, "high": 1.0},
        {"signals": [ref], "mapping": "steps", "edges": [0.5], "levels": [0.0, 1.0]},
        {"signals": [ref], "mapping": "linear", "low": 0.0, "high": 1.0, "p_low": 0.2},
    ]


def _refused(ref: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"signals": [ref, ref], "mapping": "threshold", "low": 0.0, "high": 1.0},
        {"signals": [ref], "mapping": "steps", "edges": [0.5, 0.5], "levels": [0.0, 1.0, 2.0]},
        {"signals": [ref], "mapping": "steps", "edges": [0.2, 0.8, 0.8], "levels": [0, 1, 2, 3]},
        {
            "signals": [ref],
            "mapping": "linear",
            "low": 0.0,
            "high": 1.0,
            "p_low": 0.4,
            "p_high": 0.4,
        },
        {"signals": [ref], "mapping": "linear", "low": 0.0, "high": 1.0, "p_low": 1.0},
        {"signals": [ref], "mapping": "linear", "low": 0.0, "high": 1.0, "p_high": 0.0},
    ]


def test_only_the_parameter_checks_decide_these_cases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The signal is real and pinned, so a refusal can only come from the parameter checks."""

    monkeypatch.setattr(gs, "SIGNALS_DIR", tmp_path)
    ref = _write_signal(tmp_path, "ident", [], "    return returns * 0 + 1.0")
    for params in _accepted(ref):
        assert gs.signal_exposure(INPUTS, params).notna().all()
    for params in _refused(ref):
        with pytest.raises(ValueError):
            gs.signal_exposure(INPUTS, params)


def _write_signal(directory: Path, signal_id: str, sources: list[str], body: str) -> dict[str, Any]:
    (directory / f"{signal_id}.py").write_text(
        f"def signal(returns, sources, params):\n{body}\n", encoding="utf-8"
    )
    (directory / f"{signal_id}.yaml").write_text(
        yaml.safe_dump(
            {
                "signal_id": signal_id,
                "mechanism": "test",
                "expected_sign": 1,
                "target": "return",
                "params": {},
                "sources": sources,
                "markets": "all",
                "literature": "none",
            }
        ),
        encoding="utf-8",
    )
    return {
        "id": signal_id,
        "module_sha256": chain_digest((directory / f"{signal_id}.py").read_bytes()),
        "spec_sha256": chain_digest((directory / f"{signal_id}.yaml").read_bytes()),
        "score": "sign",
    }


def test_a_source_name_is_not_a_prefix_of_another_source_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gs, "SIGNALS_DIR", tmp_path)
    ref = _write_signal(
        tmp_path,
        "reader",
        ["fred"],
        "    ok = sorted(sources['fred'].columns) == ['w', 'z']\n"
        "    return returns * 0 + (1.0 if ok else -1.0)",
    )
    inputs = INPUTS.assign(**{"source:fredx:v": 1.0, "source:fred:w": 2.0, "source:fred:z": 3.0})
    params = {"signals": [ref], "mapping": "linear", "low": 0.0, "high": 1.0}
    assert (gs.signal_exposure(inputs, params) == 1.0).all()


def _question(root: Path) -> lg.Question:
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
        screens=["S0001"],
        screen_family_size=3,
    )
    directory = root / "research/QQQ-U1"
    directory.mkdir(parents=True)
    (directory / "prereg.yaml").write_text(yaml.safe_dump(raw), encoding="utf-8")
    (directory / "strategy.py").write_text("# not executed by wiring tests\n", encoding="utf-8")
    return lg.load_question(directory)


@pytest.mark.parametrize(
    ("module_sha256", "spec_sha256", "matching"),
    [
        (REF["module_sha256"], REF["spec_sha256"], True),
        ("c" * 64, REF["spec_sha256"], False),
        (REF["module_sha256"], "d" * 64, False),
        ("c" * 64, "d" * 64, False),
    ],
)
def test_the_report_matches_a_screen_record_only_on_both_hashes(
    tmp_path: Path, module_sha256: str, spec_sha256: str, matching: bool
) -> None:
    _question(tmp_path)
    screen_ledger.append_screen_record(
        tmp_path,
        {
            "signal_id": "ident",
            "module_sha256": module_sha256,
            "spec_sha256": spec_sha256,
            "params": {},
            "markets": ["qqq"],
            "data": {"signal_snapshot_id": "sig-0123456789ab", "bench_snapshot_id": "bench-1"},
            "code": {"commit": "synthetic", "modified": False},
            "settings_sha256": "e" * 64,
            "results": {},
            "primary_p_value": None,
            "verdict": "eliminated",
            "family_size": 1,
        },
    )
    lines = report.question_report(tmp_path / "research", "QQQ-U1", load_settings().settings)
    text = "\n".join(lines)
    assert ("S0001 记录时裁决 eliminated" in text) == matching
    assert ("未筛" in text) != matching
