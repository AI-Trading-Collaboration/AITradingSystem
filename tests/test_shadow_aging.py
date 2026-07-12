from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

import pytest
from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    write_market_cache,
    write_shadow_shortlist_and_monitoring,
    write_validated_daily_advisory,
)

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DynamicV3PaperTrackingError,
    init_paper_portfolio,
    run_shadow_aging,
    track_advisory_outcome,
    update_advisory_outcome,
    validate_shadow_aging_artifact,
)


def test_shadow_aging_promotion_clock_and_downgrade_recommendation(
    tmp_path: Path,
) -> None:
    fixture = write_shadow_shortlist_and_monitoring(tmp_path, degraded=True)

    aging = run_shadow_aging(
        shadow_shortlist_id=fixture["shadow_shortlist_id"],
        config_path=paper_config_path(tmp_path),
        output_dir=tmp_path / "shadow_aging",
        shadow_shortlist_dir=fixture["shadow_shortlist_dir"],
        shadow_monitor_run_dir=fixture["shadow_monitor_run_dir"],
        consensus_drift_dir=fixture["consensus_drift_dir"],
        advisory_outcome_dir=tmp_path / "advisory_outcome",
    )
    by_candidate = {row["candidate_id"]: row for row in aging["candidate_aging_status"]}
    assert by_candidate["candidate-a"]["promotion_clock_status"] == "blocked"
    assert by_candidate["candidate-a"]["outcome_score"] is None
    assert "insufficient_candidate_outcome_evidence" in by_candidate["candidate-a"][
        "blocking_reasons"
    ]
    assert "missing_consensus_drift_evidence" in by_candidate["candidate-a"][
        "blocking_reasons"
    ]
    assert by_candidate["candidate-b"]["promotion_clock_status"] == "downgrade_recommended"
    assert aging["promotion_clock_v2_summary"]["eligible_for_review_count"] == 0
    assert aging["promotion_clock_v2_summary"]["downgrade_recommended_count"] == 1
    assert aging["promotion_clock_v2_summary"]["blocked_count"] == 1
    assert aging["manifest"]["production_candidate_generated"] is False
    assert (
        validate_shadow_aging_artifact(
            aging_id=aging["aging_id"],
            output_dir=tmp_path / "shadow_aging",
        )["status"]
        == "PASS"
    )


def test_shadow_aging_counts_only_actual_weight_changes(tmp_path: Path) -> None:
    fixture = write_shadow_shortlist_and_monitoring(tmp_path)
    for path in fixture["shadow_monitor_run_dir"].glob(
        "*/shadow_candidate_daily_results.jsonl"
    ):
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        for row in rows:
            row["target_weights"] = {"QQQ": 0.5, "SMH": 0.2, "TLT": 0.1, "CASH": 0.2}
        path.write_text(
            "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
            encoding="utf-8",
        )
    result = run_shadow_aging(
        shadow_shortlist_id=fixture["shadow_shortlist_id"],
        config_path=paper_config_path(tmp_path),
        output_dir=tmp_path / "shadow_aging",
        shadow_shortlist_dir=fixture["shadow_shortlist_dir"],
        shadow_monitor_run_dir=fixture["shadow_monitor_run_dir"],
        consensus_drift_dir=fixture["consensus_drift_dir"],
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        generated_at=datetime(2026, 7, 12, tzinfo=UTC),
    )
    assert all(row["monitor_run_count"] == 31 for row in result["candidate_aging_status"])
    assert all(row["true_rebalance_count"] == 0 for row in result["candidate_aging_status"])
    assert all(
        "insufficient_true_rebalance_count" in row["blocking_reasons"]
        for row in result["candidate_aging_status"]
    )


def test_shadow_aging_rejects_invalid_and_duplicate_selected_monitor_before_output(
    tmp_path: Path,
) -> None:
    invalid_root = tmp_path / "invalid"
    invalid = write_shadow_shortlist_and_monitoring(invalid_root)
    summary_path = (
        invalid["shadow_monitor_run_dir"] / "monitor-00" / "shadow_monitor_summary.json"
    )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["broker_action_allowed"] = True
    summary_path.write_text(json.dumps(summary, sort_keys=True), encoding="utf-8")
    with pytest.raises(DynamicV3PaperTrackingError, match="validation must PASS"):
        run_shadow_aging(
            shadow_shortlist_id=invalid["shadow_shortlist_id"],
            config_path=paper_config_path(invalid_root),
            output_dir=invalid_root / "shadow_aging",
            shadow_shortlist_dir=invalid["shadow_shortlist_dir"],
            shadow_monitor_run_dir=invalid["shadow_monitor_run_dir"],
            consensus_drift_dir=invalid["consensus_drift_dir"],
            advisory_outcome_dir=invalid_root / "advisory_outcome",
            generated_at=datetime(2026, 7, 12, tzinfo=UTC),
        )
    assert not (invalid_root / "shadow_aging").exists()

    duplicate_root = tmp_path / "duplicate"
    duplicate = write_shadow_shortlist_and_monitoring(duplicate_root)
    shutil.copytree(
        duplicate["shadow_monitor_run_dir"] / "monitor-00",
        duplicate["shadow_monitor_run_dir"] / "duplicate-monitor",
    )
    with pytest.raises(DynamicV3PaperTrackingError, match="unique monitor id and unique as_of"):
        run_shadow_aging(
            shadow_shortlist_id=duplicate["shadow_shortlist_id"],
            config_path=paper_config_path(duplicate_root),
            output_dir=duplicate_root / "shadow_aging",
            shadow_shortlist_dir=duplicate["shadow_shortlist_dir"],
            shadow_monitor_run_dir=duplicate["shadow_monitor_run_dir"],
            consensus_drift_dir=duplicate["consensus_drift_dir"],
            advisory_outcome_dir=duplicate_root / "advisory_outcome",
            generated_at=datetime(2026, 7, 12, tzinfo=UTC),
        )
    assert not (duplicate_root / "shadow_aging").exists()


def test_shadow_aging_excludes_future_monitor_by_generated_cutoff(tmp_path: Path) -> None:
    fixture = write_shadow_shortlist_and_monitoring(tmp_path)
    future_dir = fixture["shadow_monitor_run_dir"] / "future-monitor"
    shutil.copytree(fixture["shadow_monitor_run_dir"] / "monitor-00", future_dir)
    manifest_path = future_dir / "shadow_monitor_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update(
        {
            "monitor_run_id": "future-monitor",
            "as_of": "2026-08-01",
            "generated_at": "2026-08-01T12:00:00+00:00",
        }
    )
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    rows_path = future_dir / "shadow_candidate_daily_results.jsonl"
    rows = [json.loads(line) for line in rows_path.read_text(encoding="utf-8").splitlines()]
    for row in rows:
        row["as_of"] = "2026-08-01"
    rows_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    result = run_shadow_aging(
        shadow_shortlist_id=fixture["shadow_shortlist_id"],
        config_path=paper_config_path(tmp_path),
        output_dir=tmp_path / "shadow_aging",
        shadow_shortlist_dir=fixture["shadow_shortlist_dir"],
        shadow_monitor_run_dir=fixture["shadow_monitor_run_dir"],
        consensus_drift_dir=fixture["consensus_drift_dir"],
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        generated_at=datetime(2026, 7, 12, tzinfo=UTC),
    )
    assert result["manifest"]["selected_monitor_count"] == 31


def test_shadow_aging_computes_candidate_specific_outcome_from_frozen_sources(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_validated_daily_advisory(tmp_path, as_of=date(2026, 6, 8))
    outcome = track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=advisory["daily_advisory_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
    )
    prices_path, rates_path = write_market_cache(tmp_path / "cache", start="2026-06-08")
    update_advisory_outcome(
        outcome_id=outcome["outcome_id"],
        as_of=date(2026, 7, 10),
        output_dir=tmp_path / "advisory_outcome",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 7, 11, tzinfo=UTC),
    )
    target_path = Path(
        outcome["advisory_event"]["frozen_track_sources"]["daily"][
            "daily_candidate_targets"
        ]["path"]
    )
    candidate_ids = tuple(
        json.loads(line)["candidate_id"]
        for line in target_path.read_text(encoding="utf-8").splitlines()
    )
    shadow = write_shadow_shortlist_and_monitoring(
        tmp_path,
        candidate_ids=candidate_ids,
    )
    result = run_shadow_aging(
        shadow_shortlist_id=shadow["shadow_shortlist_id"],
        config_path=config_path,
        output_dir=tmp_path / "shadow_aging",
        shadow_shortlist_dir=shadow["shadow_shortlist_dir"],
        shadow_monitor_run_dir=shadow["shadow_monitor_run_dir"],
        consensus_drift_dir=shadow["consensus_drift_dir"],
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        generated_at=datetime(2026, 7, 12, tzinfo=UTC),
    )
    by_candidate = {row["candidate_id"]: row for row in result["candidate_aging_status"]}
    assert result["manifest"]["selected_outcome_count"] == 1
    assert all(by_candidate[item]["outcome_score"] is not None for item in candidate_ids)
    assert all(
        by_candidate[item]["candidate_outcome_available_window_count"] == 4
        for item in candidate_ids
    )
    assert all(
        {
            evidence["outcome_id"]
            for evidence in by_candidate[item]["outcome_evidence"]
            if evidence["status"] == "AVAILABLE"
        }
        == {outcome["outcome_id"]}
        for item in candidate_ids
    )


def test_shadow_aging_validator_replays_snapshot_after_rehash_tamper(
    tmp_path: Path,
) -> None:
    fixture = write_shadow_shortlist_and_monitoring(tmp_path)
    result = run_shadow_aging(
        shadow_shortlist_id=fixture["shadow_shortlist_id"],
        config_path=paper_config_path(tmp_path),
        output_dir=tmp_path / "shadow_aging",
        shadow_shortlist_dir=fixture["shadow_shortlist_dir"],
        shadow_monitor_run_dir=fixture["shadow_monitor_run_dir"],
        consensus_drift_dir=fixture["consensus_drift_dir"],
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        generated_at=datetime(2026, 7, 12, tzinfo=UTC),
    )
    snapshot_path = result["aging_dir"] / "shadow_aging_source_snapshot.json"
    manifest_path = result["aging_dir"] / "shadow_aging_manifest.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["selected_monitors"][0]["candidate_rows"][0]["target_weights"] = {
        "QQQ": 1.0
    }
    snapshot_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source_snapshot_checksum"] = sha256(snapshot_path.read_bytes()).hexdigest()
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    validation = validate_shadow_aging_artifact(
        aging_id=result["aging_id"], output_dir=tmp_path / "shadow_aging"
    )
    assert validation["status"] == "FAIL"
    assert any(
        check["check_id"] in {"source_snapshot_valid", "candidate_status_matches"}
        and check["passed"] is False
        for check in validation["checks"]
    )


def test_shadow_aging_legacy_artifact_is_warning_only(tmp_path: Path) -> None:
    fixture = write_shadow_shortlist_and_monitoring(tmp_path)
    result = run_shadow_aging(
        shadow_shortlist_id=fixture["shadow_shortlist_id"],
        config_path=paper_config_path(tmp_path),
        output_dir=tmp_path / "shadow_aging",
        shadow_shortlist_dir=fixture["shadow_shortlist_dir"],
        shadow_monitor_run_dir=fixture["shadow_monitor_run_dir"],
        consensus_drift_dir=fixture["consensus_drift_dir"],
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        generated_at=datetime(2026, 7, 12, tzinfo=UTC),
    )
    manifest_path = result["aging_dir"] / "shadow_aging_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("source_snapshot_schema_version")
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    validation = validate_shadow_aging_artifact(
        aging_id=result["aging_id"], output_dir=tmp_path / "shadow_aging"
    )
    assert validation["status"] == "PASS_WITH_WARNINGS"
    assert validation["source_snapshot_status"] == "LEGACY_UNSNAPSHOTTED"
