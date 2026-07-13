from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dynamic_v3_confirmation_cycle_helpers import progress_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    confirmation_progress_report_payload,
    update_confirmation_progress,
    validate_confirmation_progress_artifact,
)


def test_confirmation_progress_keeps_missing_samples_null_and_not_ready(tmp_path: Path) -> None:
    fixture = progress_fixture(tmp_path)
    progress = fixture["progress"]
    rows = {row["target_id"]: row for row in progress["target_progress"]}

    assert list(rows) == ["limited_adjustment_vs_no_trade"]
    limited = rows["limited_adjustment_vs_no_trade"]
    assert limited["available_forward_events"] == 0
    assert limited["available_by_window"] == {"1": 0, "5": 0, "10": 0, "20": 0}
    assert limited["progress_status"] == "INSUFFICIENT_EVENTS"
    assert "not_enough_forward_events" in limited["blocking_reasons"]
    assert "missing_1_5_10_20d_windows" in limited["blocking_reasons"]
    assert limited["current_metrics"]["win_rate_vs_no_trade"] is None
    assert limited["current_metrics"]["avg_relative_return"] is None

    summary = progress["target_progress_summary"]
    assert summary["ready_for_evaluation_count"] == 0
    assert summary["summary_recommendation"] == "continue_forward_tracking"

    payload = confirmation_progress_report_payload(
        progress_id=progress["progress_id"],
        output_dir=fixture["progress_dir"],
    )
    assert payload["progress_id"] == progress["progress_id"]

    validation = validate_confirmation_progress_artifact(
        progress_id=progress["progress_id"],
        output_dir=fixture["progress_dir"],
    )
    assert validation["status"] == "PASS"


def test_confirmation_progress_validator_rejects_each_output_tamper(tmp_path: Path) -> None:
    fixture = progress_fixture(tmp_path)
    for artifact_name in (
        "confirmation_progress_manifest.json",
        "target_progress.jsonl",
        "target_progress_summary.json",
        "confirmation_progress_input_snapshot.json",
        "confirmation_progress_report.md",
    ):
        path = fixture["progress"]["progress_dir"] / artifact_name
        original = path.read_text(encoding="utf-8")
        if path.suffix == ".json":
            payload = json.loads(original)
            payload["tampered"] = True
            tampered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        elif path.suffix == ".jsonl":
            payload = json.loads(original.splitlines()[0])
            payload["tampered"] = True
            tampered = json.dumps(payload, sort_keys=True) + "\n"
        else:
            tampered = original + "tamper\n"
        path.write_text(tampered, encoding="utf-8")
        validation = validate_confirmation_progress_artifact(
            progress_id=fixture["progress"]["progress_id"], output_dir=fixture["progress_dir"]
        )
        assert validation["status"] == "FAIL"
        path.write_text(original, encoding="utf-8")


def test_confirmation_progress_rejects_naive_cutoff_before_output(tmp_path: Path) -> None:
    fixture = progress_fixture(tmp_path)
    with pytest.raises(ValueError, match="timezone-aware"):
        from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
            update_confirmation_progress,
        )

        update_confirmation_progress(
            registry_id=fixture["registry"]["registry_id"],
            registry_dir=fixture["registry_dir"],
            output_dir=tmp_path / "naive-progress",
            limited_vs_notrade_dir=fixture["limited_dir"],
            consensus_risk_dir=fixture["consensus_dir"],
            generated_at=datetime(2026, 8, 1),
        )
    assert not (tmp_path / "naive-progress").exists()


def test_confirmation_progress_validator_rejects_live_source_drift(tmp_path: Path) -> None:
    fixture = progress_fixture(tmp_path)
    source_dir = Path(
        fixture["progress"]["input_snapshot"]["evidence_sources"]["limited_vs_notrade"][
            "source_dir"
        ]
    )
    report = source_dir / "limited_vs_notrade_report.md"
    report.write_text(report.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    validation = validate_confirmation_progress_artifact(
        progress_id=fixture["progress"]["progress_id"], output_dir=fixture["progress_dir"]
    )
    assert validation["status"] == "FAIL"
    with pytest.raises(ValueError, match="source validation failed"):
        update_confirmation_progress(
            registry_id=fixture["registry"]["registry_id"],
            registry_dir=fixture["registry_dir"],
            output_dir=tmp_path / "invalid-source-progress",
            limited_vs_notrade_dir=fixture["limited_dir"],
            consensus_risk_dir=fixture["consensus_dir"],
            generated_at=datetime(2026, 8, 1, tzinfo=UTC),
        )
    assert not (tmp_path / "invalid-source-progress").exists()
