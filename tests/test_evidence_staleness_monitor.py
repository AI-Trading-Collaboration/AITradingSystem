from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_owner_filtered_candidate_review_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_evidence_staleness_monitor_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_owner_filtered_candidate_review_fixture(tmp_path)
    evidence_manifest_path = Path(
        fixture["filtered_candidate_evidence"]["manifest"][
            "filtered_candidate_evidence_manifest_path"
        ]
    )
    evidence_manifest = json.loads(evidence_manifest_path.read_text(encoding="utf-8"))
    evidence_manifest["date_end"] = "2024-04-17"
    evidence_manifest_path.write_text(
        json.dumps(evidence_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    price_cache_path = tmp_path / "prices_daily.csv"
    price_cache_path.write_text(
        "date,ticker,close\n2024-04-18,QQQ,430\n2024-04-19,QQQ,431\n",
        encoding="utf-8",
    )
    market_panel_dir = tmp_path / "market_panel"
    market_panel_dir.mkdir()
    (market_panel_dir / "market_panel_2024-04-19.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "market_panel",
                "status": "PASS",
                "as_of": "2024-04-19",
                "generated_at": "2024-04-19T21:00:00+00:00",
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )

    result = readiness.run_evidence_staleness_monitor(
        as_of=date(2024, 4, 22),
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        price_cache_path=price_cache_path,
        market_panel_dir=market_panel_dir,
        evidence_id=fixture["filtered_candidate_evidence"]["evidence_id"],
        stress_backfill_id=fixture["filtered_candidate_stress_backfill"]["stress_backfill_id"],
        ab_review_id=fixture["filtered_candidate_ab_review"]["ab_review_id"],
        owner_review_id=fixture["owner_filtered_candidate_review"]["owner_review_id"],
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        output_dir=tmp_path / "evidence_staleness_monitor",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    validation = readiness.validate_evidence_staleness_monitor_artifact(
        monitor_id=result["monitor_id"],
        output_dir=tmp_path / "evidence_staleness_monitor",
    )
    report_payload = readiness.evidence_staleness_monitor_report_payload(
        monitor_id=result["monitor_id"],
        output_dir=tmp_path / "evidence_staleness_monitor",
    )
    report = result["evidence_staleness_report"]
    findings = {row["source_id"]: row for row in report["findings"]}

    assert validation["status"] == "PASS"
    assert report_payload["evidence_staleness_report"]["monitor_id"] == result["monitor_id"]
    assert report["evidence_freshness_status"] == "ACCEPTABLE"
    assert report["stale_artifacts"] == []
    assert report["blocking_artifacts"] == []
    assert report["next_refresh_action"] == "continue_with_manual_freshness_note"
    assert findings["price_data"]["severity"] == "ACCEPTABLE"
    assert findings["market_panel_data"]["severity"] == "ACCEPTABLE"
    assert findings["signal_artifact"]["timestamp_basis"] == "evidence_date_end_or_generated_at"
    assert findings["stress_backfill_result"]["severity"] == "FRESH"
    assert findings["ab_review"]["severity"] == "FRESH"
    assert findings["owner_review"]["severity"] == "FRESH"
    assert "evidence_freshness_status" in result["reader_brief_section"]
    assert result["manifest"]["data_downloaded_by_monitor"] is False
    assert result["manifest"]["pipelines_executed_by_monitor"] is False
    assert_research_safe(result["manifest"])


def test_evidence_staleness_future_timestamp_blocks() -> None:
    finding = readiness._evidence_freshness_finding(
        source_id="signal_artifact",
        source_label="Filtered candidate signal evidence",
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        timestamp=date(2024, 4, 23),
        timestamp_basis="generated_at",
        source_path=None,
        artifact_id="future_artifact",
        rule={"fresh_days": 7, "acceptable_days": 14, "blocking_days": 30},
        as_of=date(2024, 4, 22),
    )

    assert finding["raw_age_days"] == -1
    assert finding["age_days"] == -1
    assert finding["timestamp_relation"] == "AFTER_AS_OF"
    assert finding["severity"] == "BLOCKING"
