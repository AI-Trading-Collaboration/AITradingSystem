from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.qqq_options_research.exact_signal_package_admission import (
    ExactSignalPackageAdmissionPolicy,
    audit_first_layer_signal_source,
    load_exact_signal_package_admission_policy,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day


def test_exact_signal_package_admission_policy_replays_frozen_scope() -> None:
    loaded = load_exact_signal_package_admission_policy()

    assert loaded.policy.research_window.expected_session_count == 1202
    assert loaded.policy.signal_mapping == {
        "constructive": "LONG_CALL",
        "defensive": "FLAT",
        "neutral": "FLAT",
        "risk_off": "FLAT",
        "risk_on": "LONG_CALL",
    }
    assert loaded.policy.safety.quantconnect_backtest_allowed is False
    assert loaded.policy.safety.orders == 0


def test_exact_signal_package_admission_policy_rejects_mapping_drift() -> None:
    loaded = load_exact_signal_package_admission_policy()
    payload = loaded.policy.model_dump(mode="python")
    payload["signal_mapping"] = {**payload["signal_mapping"], "risk_off": "LONG_CALL"}

    with pytest.raises(ValueError, match="mapping drifted"):
        ExactSignalPackageAdmissionPolicy.model_validate(payload)


def test_signal_source_audit_accepts_exact_one_row_per_session(tmp_path: Path) -> None:
    loaded = load_exact_signal_package_admission_policy()
    source_path = tmp_path / "first_layer_composer_v2_predictions.csv"
    sessions = _sessions(date(2021, 2, 22), date(2025, 12, 2))
    pd.DataFrame(_rows(sessions)).to_csv(source_path, index=False)

    audit = audit_first_layer_signal_source(
        loaded_policy=loaded,
        source_path=source_path,
        project_root=tmp_path,
    )

    assert audit.admission_status == "PASS"
    assert audit.unique_session_count == 1202
    assert audit.blocker_codes == ()


def test_signal_source_audit_rejects_gap_duplicate_and_calendar_timing(
    tmp_path: Path,
) -> None:
    loaded = load_exact_signal_package_admission_policy()
    source_path = tmp_path / "first_layer_composer_v2_predictions.csv"
    sessions = _sessions(date(2021, 2, 22), date(2025, 12, 2))
    rows = _rows(sessions[1:])
    rows.append(dict(rows[0]))
    rows[0]["decision_at"] = rows[0]["date"]
    pd.DataFrame(rows).to_csv(source_path, index=False)

    audit = audit_first_layer_signal_source(
        loaded_policy=loaded,
        source_path=source_path,
        project_root=tmp_path,
    )

    assert audit.admission_status == "REJECT"
    assert audit.missing_session_count == 1
    assert audit.duplicate_session_count == 1
    assert audit.invalid_timing_row_count == 1
    assert audit.blocker_codes == (
        "SOURCE_PRIMARY_START_NOT_COVERED",
        "SOURCE_SESSION_COVERAGE_MISMATCH",
        "SOURCE_DUPLICATE_SESSION",
        "SOURCE_TIMING_INVALID",
    )


def _sessions(start: date, end: date) -> tuple[date, ...]:
    values: list[date] = []
    current = start
    while current <= end:
        if is_us_equity_trading_day(current):
            values.append(current)
        current += timedelta(days=1)
    return tuple(values)


def _next_session(value: date) -> date:
    candidate = value + timedelta(days=1)
    while not is_us_equity_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _rows(sessions: tuple[date, ...]) -> list[dict[str, str]]:
    return [
        {
            "date": session.isoformat(),
            "model_id": "first_layer_composer_v2",
            "trend_state": "constructive",
            "research_window_id": "exact_three_asset_validated",
            "known_at": session.isoformat(),
            "available_at": session.isoformat(),
            "decision_at": _next_session(session).isoformat(),
        }
        for session in sessions
    ]
