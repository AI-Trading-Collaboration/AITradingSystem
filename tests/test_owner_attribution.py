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
    write_validated_owner_review,
)

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DynamicV3PaperTrackingError,
    init_paper_portfolio,
    run_owner_attribution,
    track_advisory_outcome,
    update_advisory_outcome,
    validate_owner_attribution_artifact,
)


def test_owner_attribution_links_reviews_and_keeps_insufficient_outcome_data(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    review = write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 7),
    )
    track_advisory_outcome(
        daily_advisory_id=review["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=review["daily_advisory_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 7, 15, tzinfo=UTC),
    )

    attribution = run_owner_attribution(
        output_dir=tmp_path / "owner_attribution",
        owner_review_dir=tmp_path / "owner_review_journal",
        outcome_dir=tmp_path / "advisory_outcome",
    )

    assert attribution["owner_decision_summary"]["total_reviews"] == 1
    assert attribution["owner_decision_summary"]["monitor"] == 1
    assert attribution["manifest"]["linked_outcome_count"] == 1
    matrix_rows = attribution["advisory_acceptance_matrix"]["by_recommended_action"]
    assert sum(row["accepted_monitor"] for row in matrix_rows.values()) == 1
    assert attribution["decision_outcome_comparison"]["status"] == "INSUFFICIENT_DATA"
    group = attribution["decision_outcome_comparison"]["decision_groups"][0]
    assert group["review_count"] == 1
    assert group["linked_outcome_count"] == 1
    assert group["available_outcome_count"] == 0
    assert group["available_window_count"] == 0
    assert group["avg_5d_relative_to_no_trade"] is None
    assert group["avg_20d_relative_to_no_trade"] is None
    assert group["insufficient_reason"] == "NO_AVAILABLE_OUTCOME_WINDOW"
    assert attribution["manifest"]["status"] == "PASS"
    assert attribution["manifest"]["evidence_status"] == "INSUFFICIENT_DATA"
    assert attribution["manifest"]["broker_action_taken"] is False
    assert (
        validate_owner_attribution_artifact(
            attribution_id=attribution["attribution_id"],
            output_dir=tmp_path / "owner_attribution",
        )["status"]
        == "PASS"
    )


def test_owner_attribution_uses_review_outcome_and_window_as_distinct_units(
    tmp_path: Path,
) -> None:
    fixture = _owner_outcome_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "cache", start="2026-06-08")
    update_advisory_outcome(
        outcome_id=fixture["outcome"]["outcome_id"],
        as_of=date(2026, 7, 10),
        output_dir=tmp_path / "advisory_outcome",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    result = run_owner_attribution(
        output_dir=tmp_path / "owner_attribution",
        owner_review_dir=tmp_path / "owner_review_journal",
        outcome_dir=tmp_path / "advisory_outcome",
        generated_at=datetime(2026, 7, 16, tzinfo=UTC),
    )
    comparison = result["decision_outcome_comparison"]
    group = comparison["decision_groups"][0]
    assert comparison["status"] == "AVAILABLE"
    assert comparison["review_count"] == 1
    assert comparison["linked_outcome_count"] == 1
    assert comparison["available_outcome_count"] == 1
    assert comparison["available_window_count"] == 4
    assert group["review_count"] == 1
    assert group["available_outcome_count"] == 1
    assert group["available_window_count"] == 4
    assert group["avg_5d_relative_to_no_trade"] == pytest.approx(0.0)
    assert group["avg_20d_relative_to_no_trade"] == pytest.approx(0.0)
    assert group["insufficient_reason"] == ""
    assert result["manifest"]["evidence_status"] == "AVAILABLE"


def test_owner_attribution_fails_before_output_for_invalid_owner_source(
    tmp_path: Path,
) -> None:
    _owner_outcome_fixture(tmp_path)
    journal_path = tmp_path / "owner_review_journal" / "owner_review_journal.jsonl"
    record = json.loads(journal_path.read_text(encoding="utf-8").splitlines()[0])
    record["owner_decision"] = "no_trade"
    journal_path.write_text(json.dumps(record, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(DynamicV3PaperTrackingError, match="event-chain replay"):
        run_owner_attribution(
            output_dir=tmp_path / "owner_attribution",
            owner_review_dir=tmp_path / "owner_review_journal",
            outcome_dir=tmp_path / "advisory_outcome",
            generated_at=datetime(2026, 6, 9, tzinfo=UTC),
        )
    assert not (tmp_path / "owner_attribution").exists()


def test_owner_attribution_fails_before_output_for_invalid_or_duplicate_outcome(
    tmp_path: Path,
) -> None:
    fixture = _owner_outcome_fixture(tmp_path)
    report_path = fixture["outcome"]["outcome_dir"] / "advisory_outcome_report.md"
    report_path.write_text(report_path.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    with pytest.raises(DynamicV3PaperTrackingError, match="validation must PASS"):
        run_owner_attribution(
            output_dir=tmp_path / "invalid_attribution",
            owner_review_dir=tmp_path / "owner_review_journal",
            outcome_dir=tmp_path / "advisory_outcome",
            generated_at=datetime(2026, 6, 9, tzinfo=UTC),
        )
    assert not (tmp_path / "invalid_attribution").exists()

    clean_root = tmp_path / "duplicate_case"
    duplicate_fixture = _owner_outcome_fixture(clean_root)
    shutil.copytree(
        duplicate_fixture["outcome"]["outcome_dir"],
        clean_root / "advisory_outcome" / "duplicate-outcome",
    )
    with pytest.raises(DynamicV3PaperTrackingError, match="multiple advisory outcomes"):
        run_owner_attribution(
            output_dir=clean_root / "owner_attribution",
            owner_review_dir=clean_root / "owner_review_journal",
            outcome_dir=clean_root / "advisory_outcome",
            generated_at=datetime(2026, 6, 9, tzinfo=UTC),
        )
    assert not (clean_root / "owner_attribution").exists()


def test_owner_attribution_rejects_future_cutoff(tmp_path: Path) -> None:
    _owner_outcome_fixture(tmp_path)
    with pytest.raises(DynamicV3PaperTrackingError, match="attribution cutoff"):
        run_owner_attribution(
            output_dir=tmp_path / "owner_attribution",
            owner_review_dir=tmp_path / "owner_review_journal",
            outcome_dir=tmp_path / "advisory_outcome",
            generated_at=datetime(2026, 6, 8, 12, 30, tzinfo=UTC),
        )
    assert not (tmp_path / "owner_attribution").exists()
    with pytest.raises(DynamicV3PaperTrackingError, match="tracking/update time"):
        run_owner_attribution(
            output_dir=tmp_path / "outcome_future_attribution",
            owner_review_dir=tmp_path / "owner_review_journal",
            outcome_dir=tmp_path / "advisory_outcome",
            generated_at=datetime(2026, 6, 8, 14, tzinfo=UTC),
        )
    assert not (tmp_path / "outcome_future_attribution").exists()


def test_owner_attribution_validator_replays_snapshots_after_rehash_tamper(
    tmp_path: Path,
) -> None:
    _owner_outcome_fixture(tmp_path)
    result = run_owner_attribution(
        output_dir=tmp_path / "owner_attribution",
        owner_review_dir=tmp_path / "owner_review_journal",
        outcome_dir=tmp_path / "advisory_outcome",
        generated_at=datetime(2026, 6, 9, tzinfo=UTC),
    )
    snapshot_path = result["attribution_dir"] / "owner_review_source_snapshot.json"
    manifest_path = result["attribution_dir"] / "owner_attribution_manifest.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["selected_reviews"][0]["review"]["owner_decision"] = "no_trade"
    snapshot_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["owner_review_source_snapshot_checksum"] = sha256(
        snapshot_path.read_bytes()
    ).hexdigest()
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    validation = validate_owner_attribution_artifact(
        attribution_id=result["attribution_id"],
        output_dir=tmp_path / "owner_attribution",
    )
    assert validation["status"] == "FAIL"
    assert any(
        not check["passed"]
        and check["check_id"]
        in {
            "snapshot_content_valid",
            "decision_summary_matches",
            "outcome_comparison_matches",
        }
        for check in validation["checks"]
    )


def test_owner_attribution_legacy_artifact_is_read_only_warning(tmp_path: Path) -> None:
    _owner_outcome_fixture(tmp_path)
    result = run_owner_attribution(
        output_dir=tmp_path / "owner_attribution",
        owner_review_dir=tmp_path / "owner_review_journal",
        outcome_dir=tmp_path / "advisory_outcome",
        generated_at=datetime(2026, 6, 9, tzinfo=UTC),
    )
    manifest_path = result["attribution_dir"] / "owner_attribution_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("source_snapshot_schema_version")
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    validation = validate_owner_attribution_artifact(
        attribution_id=result["attribution_id"],
        output_dir=tmp_path / "owner_attribution",
    )
    assert validation["status"] == "PASS_WITH_WARNINGS"
    assert validation["source_snapshot_status"] == "LEGACY_UNSNAPSHOTTED"


def _owner_outcome_fixture(tmp_path: Path) -> dict[str, object]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    review = write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
    )
    outcome = track_advisory_outcome(
        daily_advisory_id=review["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=review["daily_advisory_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
    )
    return {"config_path": config_path, "review": review, "outcome": outcome}
