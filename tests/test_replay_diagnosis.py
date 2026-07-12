from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_historical_replay_helpers import (
    build_replay_review_chain,
    prepare_replay_test_environment,
)

from ai_trading_system.etf_portfolio import dynamic_v3_historical_replay as replay_module
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DynamicV3HistoricalReplayError,
    run_replay_diagnosis,
    validate_replay_diagnosis_artifact,
)


def _run_diagnosis(paths: dict[str, Path], *, chain_time: datetime) -> dict[str, Any]:
    chain = build_replay_review_chain(
        paths,
        backfill_generated_at=chain_time,
        chain_generated_at=chain_time,
    )
    diagnosis = run_replay_diagnosis(
        inventory_id=chain["inventory"]["inventory_id"],
        replay_id=chain["replay"]["replay_id"],
        backfill_id=chain["backfill"]["backfill_id"],
        sim_id=chain["sim"]["sim_id"],
        review_id=chain["review"]["review_id"],
        inventory_dir=paths["inventory_dir"],
        replay_dir=paths["historical_replay_dir"],
        backfill_dir=paths["backfill_dir"],
        sim_dir=paths["paper_sim_dir"],
        review_dir=paths["performance_review_dir"],
        output_dir=paths["diagnosis_dir"],
        generated_at=chain_time,
    )
    return {**chain, "diagnosis": diagnosis}


def test_replay_diagnosis_classifies_pending_reasons_and_counts_paper_events(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    chain = build_replay_review_chain(
        paths,
        backfill_generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    diagnosis = run_replay_diagnosis(
        inventory_id=chain["inventory"]["inventory_id"],
        replay_id=chain["replay"]["replay_id"],
        backfill_id=chain["backfill"]["backfill_id"],
        sim_id=chain["sim"]["sim_id"],
        review_id=chain["review"]["review_id"],
        inventory_dir=paths["inventory_dir"],
        replay_dir=paths["historical_replay_dir"],
        backfill_dir=paths["backfill_dir"],
        sim_dir=paths["paper_sim_dir"],
        review_dir=paths["performance_review_dir"],
        output_dir=paths["diagnosis_dir"],
        generated_at=datetime(2026, 7, 21, tzinfo=UTC),
    )

    coverage = diagnosis["coverage_breakdown"]
    reasons = {row["reason"]: row for row in diagnosis["pending_reason_summary"]["pending_reasons"]}
    assert coverage["inventory"]["pit_safe"] == 1
    assert coverage["inventory"]["pit_warning"] == 0
    assert coverage["inventory"]["pit_unsafe"] == 0
    assert coverage["replay"]["replayed_events"] == 1
    assert coverage["backfill"]["available_windows"] > 0
    assert coverage["backfill"]["pending_windows"] > 0
    assert coverage["paper_sim"]["event_count"] == len(chain["sim"]["state_history"])
    assert reasons["future_window_not_reached"]["blocking"] is False
    assert reasons["future_window_not_reached"]["count_units"] == {
        "outcome_variant_window": reasons["future_window_not_reached"]["count"]
    }
    assert diagnosis["manifest"]["can_enter_variant_comparison"] is False
    assert diagnosis["manifest"]["variant_comparison_evidence_status"] == (
        "INSUFFICIENT_DIRECTIONAL_EVIDENCE"
    )
    assert all(
        row["validation_status"] == "PASS" and row["manifest_checksum"]
        for row in diagnosis["artifact_health_matrix"]
    )

    validation = validate_replay_diagnosis_artifact(
        diagnosis_id=diagnosis["diagnosis_id"],
        output_dir=paths["diagnosis_dir"],
    )
    assert validation["status"] == "PASS"


def test_replay_diagnosis_has_clean_empty_reason_set_without_unknown_blocker(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    del tmp_path, monkeypatch
    summary = replay_module._replay_pending_reason_summary(
        inventory_rows=[],
        replay_summary={"replay_event_count": 1},
        backfill_manifest={"available_count": 1},
        outcome_rows=[{"outcome_status": "AVAILABLE"}],
        sim_summary={"simulation_status": "AVAILABLE"},
        review_manifest={"status": "AVAILABLE"},
    )
    assert summary["pending_reasons"] == []


def test_replay_diagnosis_fails_before_output_for_time_or_invalid_source(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    artifacts = _run_diagnosis(
        paths,
        chain_time=datetime(2026, 7, 20, tzinfo=UTC),
    )
    completed = Path(artifacts["diagnosis"]["diagnosis_dir"])
    completed.rename(tmp_path / "completed_diagnosis")
    paths["diagnosis_dir"].rmdir()
    with pytest.raises(DynamicV3HistoricalReplayError, match="must not precede"):
        run_replay_diagnosis(
            inventory_id=artifacts["inventory"]["inventory_id"],
            replay_id=artifacts["replay"]["replay_id"],
            backfill_id=artifacts["backfill"]["backfill_id"],
            sim_id=artifacts["sim"]["sim_id"],
            review_id=artifacts["review"]["review_id"],
            inventory_dir=paths["inventory_dir"],
            replay_dir=paths["historical_replay_dir"],
            backfill_dir=paths["backfill_dir"],
            sim_dir=paths["paper_sim_dir"],
            review_dir=paths["performance_review_dir"],
            output_dir=paths["diagnosis_dir"],
            generated_at=datetime(2026, 7, 19, tzinfo=UTC),
        )
    review_manifest = Path(artifacts["review"]["review_dir"]) / "replay_performance_manifest.json"
    payload = json.loads(review_manifest.read_text(encoding="utf-8"))
    payload["status"] = "PENDING"
    review_manifest.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(DynamicV3HistoricalReplayError, match="source validation must PASS"):
        run_replay_diagnosis(
            inventory_id=artifacts["inventory"]["inventory_id"],
            replay_id=artifacts["replay"]["replay_id"],
            backfill_id=artifacts["backfill"]["backfill_id"],
            sim_id=artifacts["sim"]["sim_id"],
            review_id=artifacts["review"]["review_id"],
            inventory_dir=paths["inventory_dir"],
            replay_dir=paths["historical_replay_dir"],
            backfill_dir=paths["backfill_dir"],
            sim_dir=paths["paper_sim_dir"],
            review_dir=paths["performance_review_dir"],
            output_dir=paths["diagnosis_dir"],
            generated_at=datetime(2026, 7, 21, tzinfo=UTC),
        )
    assert not paths["diagnosis_dir"].exists()


@pytest.mark.parametrize("tamper_target", ["snapshot", "coverage", "report", "source"])
def test_replay_diagnosis_validator_recomputes_all_views(
    tmp_path: Path,
    monkeypatch: Any,
    tamper_target: str,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    artifacts = _run_diagnosis(
        paths,
        chain_time=datetime(2026, 7, 20, tzinfo=UTC),
    )
    diagnosis = artifacts["diagnosis"]
    diagnosis_dir = Path(diagnosis["diagnosis_dir"])
    if tamper_target == "snapshot":
        snapshot_path = diagnosis_dir / "replay_diagnosis_source_snapshot.json"
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot["replay_id"] = "tampered"
        snapshot_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest_path = diagnosis_dir / "replay_diagnosis_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["source_snapshot_checksum"] = sha256(snapshot_path.read_bytes()).hexdigest()
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    elif tamper_target == "coverage":
        path = diagnosis_dir / "replay_coverage_breakdown.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["backfill"]["available_windows"] += 1
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    elif tamper_target == "report":
        path = diagnosis_dir / "replay_diagnosis_report.md"
        path.write_text(path.read_text(encoding="utf-8") + "tamper", encoding="utf-8")
    else:
        path = Path(artifacts["sim"]["sim_dir"]) / "simulated_performance_summary.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["total_return"] = 99.0
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    assert (
        validate_replay_diagnosis_artifact(
            diagnosis_id=diagnosis["diagnosis_id"],
            output_dir=paths["diagnosis_dir"],
        )["status"]
        == "FAIL"
    )
