from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot
from trading_engine.test_weight_tuning_failure_attribution import (
    write_weight_tuning_failure_artifact,
)


def test_reader_brief_displays_weight_tuning_failure_root_cause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 28)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    write_weight_tuning_failure_artifact(tmp_path, as_of=as_of)
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_reader_brief_payload(
        as_of=as_of,
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["weight_tuning_failure_status"] == "NO_CANDIDATE_EXPLAINED"
    assert review["weight_tuning_failure_root_cause"] == "portfolio_turnover_too_high"
    assert review["weight_tuning_failure_top_reason"] == "turnover_guardrail_failed"
    assert "turnover guardrails" in review["weight_tuning_failure_summary"]
