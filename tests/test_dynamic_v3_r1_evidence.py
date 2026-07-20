from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import dynamic_v3_parameter_research as legacy
from ai_trading_system.etf_portfolio.dynamic_v3_r1_evidence import (
    _build_walk_forward_report,
    _chronology_summary,
    _effective_walk_forward_windows,
    _trading_dates,
)


def test_trading_dates_use_price_date_column_instead_of_range_index() -> None:
    prices = pd.DataFrame(
        {
            "date": ["2026-01-05", "2026-01-02", "2026-01-05"],
            "symbol": ["QQQ", "QQQ", "SPY"],
        }
    )

    assert _trading_dates(prices) == [date(2026, 1, 2), date(2026, 1, 5)]


def test_effective_walk_forward_window_applies_purge_and_embargo(monkeypatch) -> None:
    monkeypatch.setattr(
        legacy,
        "walk_forward_windows",
        lambda _config: [
            {
                "train_start": "2026-01-02",
                "train_end": "2026-01-09",
                "test_start": "2026-01-12",
                "test_end": "2026-01-16",
            }
        ],
    )
    trading_dates = [stamp.date() for stamp in pd.bdate_range("2026-01-02", "2026-01-16")]

    windows = _effective_walk_forward_windows(
        config=object(),
        trading_dates=trading_dates,
        policy={"purge_trading_days": 1, "embargo_trading_days": 1},
    )

    assert windows[0]["effective_train_end"] == "2026-01-08"
    assert windows[0]["effective_test_start"] == "2026-01-13"
    assert windows[0]["purge_trading_days"] == 1
    assert windows[0]["embargo_trading_days"] == 1


def test_chronology_summary_requires_next_session_execution() -> None:
    trading_dates = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]
    valid = [
        {
            "signal_date": "2026-01-02",
            "execution_date": "2026-01-05",
            "return_date": "2026-01-06",
        }
    ]
    invalid = [
        {
            "signal_date": "2026-01-05",
            "execution_date": "2026-01-05",
            "return_date": "2026-01-06",
        }
    ]

    assert _chronology_summary(valid, trading_dates) == {
        "status": "PASS",
        "row_count": 1,
        "invalid_row_count": 0,
        "execution_lag_trading_days": {1: 1},
        "outcome_lag_trading_days": {1: 1},
    }
    assert _chronology_summary(invalid, trading_dates)["status"] == "FAIL"


def test_walk_forward_report_never_claims_unbiased_oos() -> None:
    summary = {
        "window_index": 1,
        "phase": "test",
        "candidate_id": "candidate-a",
        "evidence_status": "COMPLETE",
        "gate": legacy.GATE_REJECT,
    }
    train_summary = {**summary, "phase": "train", "gate": legacy.GATE_OBSERVE_ONLY}
    report = _build_walk_forward_report(
        wf_id="r1-wf_test",
        source={
            "source_sweep_id": "sweep-test",
            "config": SimpleNamespace(
                out_of_sample=SimpleNamespace(
                    holdout_start=date(2025, 1, 1),
                    holdout_end=date(2026, 12, 31),
                )
            ),
        },
        selected_results=[{"candidate_id": "candidate-a"}],
        windows=[
            {
                "effective_train_start": "2025-01-02",
                "effective_test_end": "2026-01-02",
            }
        ],
        evaluation_index=[{"summary": train_summary}, {"summary": summary}],
        preflight={"status": "PASS"},
        generated=datetime(2026, 7, 20, tzinfo=UTC),
    )

    assert report["status"] == "FAIL_RESEARCH_EVIDENCE"
    assert report["source_selection_contamination"] is True
    assert report["locked_holdout_overlap"] is True
    assert report["oos_summary"]["unbiased_oos_claim_allowed"] is False
    assert report["oos_summary"]["primary_window_conclusion_allowed"] is False


def test_r1_cli_commands_are_registered() -> None:
    walk_forward = CliRunner().invoke(
        app,
        ["etf", "dynamic-v3-rescue", "walk-forward", "--help"],
        color=False,
    )
    rescue = CliRunner().invoke(
        app,
        ["etf", "dynamic-v3-rescue", "--help"],
        color=False,
    )
    robustness = CliRunner().invoke(
        app,
        ["etf", "dynamic-v3-rescue", "robustness", "--help"],
        color=False,
    )

    assert walk_forward.exit_code == 0
    assert "r1-run" in walk_forward.output
    assert rescue.exit_code == 0
    assert "validate-r1-walk-forward" in rescue.output
    assert "validate-r1-robustness" in rescue.output
    assert robustness.exit_code == 0
    assert "r1-run" in robustness.output
