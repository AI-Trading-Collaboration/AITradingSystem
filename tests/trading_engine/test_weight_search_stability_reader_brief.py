from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.parameters.weight_stability import (
    write_weight_stability_summary,
)
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot
from trading_engine.weight_stability_helpers import sample_weight_stability_payload


@pytest.mark.parametrize(
    ("candidate_status", "expected_text"),
    [
        ("watch", "found a shadow-only candidate"),
        ("no_candidate", "still did not find"),
    ],
)
def test_reader_brief_weight_stability_summary_sentences(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    candidate_status: str,
    expected_text: str,
) -> None:
    as_of = date(2026, 5, 28)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    _write_weight_stability_artifact(
        tmp_path,
        as_of,
        sample_weight_stability_payload(
            as_of=as_of,
            candidate_status=candidate_status,
        ),
    )

    summary = reader_brief._weight_stability_review_summary(as_of)

    assert summary["status"] == "LIMITED"
    assert summary["candidate_status"] == candidate_status
    assert expected_text in summary["summary_sentence"]


def test_reader_brief_displays_weight_stability_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 28)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    _write_weight_stability_artifact(
        tmp_path,
        as_of,
        sample_weight_stability_payload(as_of=as_of),
    )
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_reader_brief_payload(
        as_of=as_of,
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["weight_stability_status"] == "LIMITED"
    assert review["weight_stability_candidate_status"] == "no_candidate"
    assert review["weight_stability_candidates_generated"] == 120
    assert review["weight_stability_rejected_by_stability"] == 60
    assert review["weight_stability_rejected_by_turnover_prefilter"] == 20
    assert review["weight_stability_turnover_failures_reduced"] is True
    assert "still did not find" in review["weight_stability_summary"]


def _write_weight_stability_artifact(
    tmp_path: Path,
    as_of: date,
    payload: dict[str, object],
) -> None:
    artifact_dir = tmp_path / "artifacts" / "weight_stability" / as_of.isoformat()
    write_weight_stability_summary(
        payload,
        artifact_dir / "weight_stability_summary.json",
        artifact_dir / "weight_stability_summary.md",
    )
