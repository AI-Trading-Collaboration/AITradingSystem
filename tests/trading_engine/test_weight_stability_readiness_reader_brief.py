from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.parameters.weight_stability_readiness import (
    write_weight_stability_readiness_summary,
)
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot
from trading_engine.weight_stability_readiness_helpers import (
    sample_weight_stability_readiness_payload,
)


def test_reader_brief_displays_weight_stability_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 29)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    artifact_dir = tmp_path / "artifacts" / "weight_stability_readiness" / as_of.isoformat()
    write_weight_stability_readiness_summary(
        sample_weight_stability_readiness_payload(as_of=as_of),
        artifact_dir / "weight_stability_readiness_summary.json",
        artifact_dir / "weight_stability_readiness_summary.md",
    )
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_reader_brief_payload(
        as_of=as_of,
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["weight_stability_readiness_status"] == "RECOVERY_FAILED"
    assert review["weight_stability_readiness_can_run"] is False
    assert "price_coverage" in review["weight_stability_readiness_blocking_checks"]
    assert "blocked before backtest" in review["weight_stability_readiness_summary"]
