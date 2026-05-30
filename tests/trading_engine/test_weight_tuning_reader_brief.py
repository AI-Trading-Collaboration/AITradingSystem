from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.parameters.weight_tuning import (
    write_weight_tuning_summary,
)
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot
from trading_engine.weight_tuning_helpers import sample_weight_tuning_payload


@pytest.mark.parametrize(
    ("status", "candidate_status", "expected_text"),
    [
        ("LIMITED", "watch", "produced a shadow-only candidate"),
        ("NO_CANDIDATE", "rejected", "did not find a candidate"),
        ("INSUFFICIENT_DATA", "needs_more_data", "could not run"),
    ],
)
def test_reader_brief_weight_tuning_summary_sentences(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    candidate_status: str,
    expected_text: str,
) -> None:
    as_of = date(2026, 5, 28)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    _write_weight_tuning_artifact(
        tmp_path,
        as_of,
        sample_weight_tuning_payload(
            as_of=as_of,
            status=status,
            candidate_status=candidate_status,
        ),
    )

    summary = reader_brief._weight_tuning_review_summary(as_of)

    assert summary["status"] == status
    assert summary["candidate_status"] == candidate_status
    assert expected_text in summary["summary_sentence"]


def test_reader_brief_displays_weight_tuning_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 28)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    _write_weight_tuning_artifact(
        tmp_path,
        as_of,
        sample_weight_tuning_payload(as_of=as_of),
    )
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_reader_brief_payload(
        as_of=as_of,
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["weight_tuning_status"] == "NO_CANDIDATE"
    assert review["weight_tuning_candidate_status"] == "rejected"
    assert review["weight_tuning_candidates_evaluated"] == 240
    assert review["weight_tuning_guardrail_status"] == "FAIL"
    assert "did not find a candidate" in review["weight_tuning_summary"]


def _write_weight_tuning_artifact(
    tmp_path: Path,
    as_of: date,
    payload: dict[str, object],
) -> None:
    artifact_dir = tmp_path / "artifacts" / "weight_tuning" / as_of.isoformat()
    write_weight_tuning_summary(
        payload,
        artifact_dir / "weight_tuning_summary.json",
        artifact_dir / "weight_tuning_summary.md",
    )
