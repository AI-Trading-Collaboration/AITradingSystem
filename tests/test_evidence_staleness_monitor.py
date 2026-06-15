from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_paper_shadow_protocol_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_daily as daily
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_drift as drift
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly


def test_evidence_staleness_monitor_builds_and_validates(tmp_path: Path) -> None:
    fixture = _paper_shadow_freshness_fixture(tmp_path)
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
        paper_shadow_daily_id=fixture["paper_shadow_daily"]["observation_id"],
        paper_shadow_drift_monitor_id=fixture["paper_shadow_drift"]["monitor_id"],
        paper_shadow_weekly_review_id=fixture["paper_shadow_weekly"]["weekly_review_id"],
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        paper_shadow_daily_dir=tmp_path / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=tmp_path / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=tmp_path / "paper_shadow_weekly_review",
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
    assert report["missing_artifacts"] == []
    assert report["next_refresh_action"] == "continue_with_manual_freshness_note"
    assert report["safe_to_continue_shadow"] is True
    assert report["safety_boundary_status"] == "PASS"
    assert findings["price_data"]["severity"] == "ACCEPTABLE"
    assert findings["market_panel_data"]["severity"] == "ACCEPTABLE"
    assert findings["signal_artifact"]["timestamp_basis"] == "evidence_date_end_or_generated_at"
    assert findings["stress_backfill_result"]["severity"] == "FRESH"
    assert findings["ab_review"]["severity"] == "FRESH"
    assert findings["owner_review"]["severity"] == "FRESH"
    assert findings["paper_shadow_daily_observation"]["severity"] == "FRESH"
    assert findings["paper_shadow_drift_monitor"]["severity"] == "FRESH"
    assert findings["paper_shadow_weekly_review"]["severity"] == "FRESH"
    assert findings["paper_shadow_weekly_review"]["timestamp_basis"] == (
        "paper_shadow_weekly_week_end_or_generated_at"
    )
    assert "evidence_freshness_status" in result["reader_brief_section"]
    assert "missing_artifacts" in result["reader_brief_section"]
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


def test_evidence_staleness_missing_weekly_review_blocks(tmp_path: Path) -> None:
    fixture = _paper_shadow_freshness_fixture(tmp_path)
    price_cache_path = tmp_path / "prices_daily.csv"
    price_cache_path.write_text(
        "date,ticker,close\n2024-04-22,QQQ,431\n",
        encoding="utf-8",
    )
    market_panel_dir = tmp_path / "market_panel"
    market_panel_dir.mkdir()
    (market_panel_dir / "market_panel_2024-04-22.json").write_text(
        json.dumps({"status": "PASS", "as_of": "2024-04-22"}),
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
        paper_shadow_daily_id=fixture["paper_shadow_daily"]["observation_id"],
        paper_shadow_drift_monitor_id=fixture["paper_shadow_drift"]["monitor_id"],
        paper_shadow_weekly_review_id="missing-weekly-review",
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        paper_shadow_daily_dir=tmp_path / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=tmp_path / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=tmp_path / "paper_shadow_weekly_review",
        output_dir=tmp_path / "evidence_staleness_monitor_missing",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    report = result["evidence_staleness_report"]
    findings = {row["source_id"]: row for row in report["findings"]}

    assert report["evidence_freshness_status"] == "BLOCKING"
    assert report["safe_to_continue_shadow"] is False
    assert report["missing_artifacts"] == ["paper_shadow_weekly_review"]
    assert findings["paper_shadow_weekly_review"]["missing"] is True
    assert result["evidence_staleness_validation"]["status"] == "PASS"


def test_evidence_staleness_discovers_latest_weekly_review_artifact(tmp_path: Path) -> None:
    fixture = _paper_shadow_freshness_fixture(tmp_path)
    evidence_manifest_path = Path(
        fixture["filtered_candidate_evidence"]["manifest"][
            "filtered_candidate_evidence_manifest_path"
        ]
    )
    evidence_manifest = json.loads(evidence_manifest_path.read_text(encoding="utf-8"))
    evidence_manifest["date_end"] = "2024-04-22"
    evidence_manifest_path.write_text(
        json.dumps(evidence_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    price_cache_path = tmp_path / "prices_daily.csv"
    price_cache_path.write_text(
        "date,ticker,close\n2024-04-22,QQQ,431\n",
        encoding="utf-8",
    )
    market_panel_dir = tmp_path / "market_panel"
    market_panel_dir.mkdir()
    (market_panel_dir / "market_panel_2024-04-22.json").write_text(
        json.dumps({"status": "PASS", "as_of": "2024-04-22"}),
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
        paper_shadow_daily_id=fixture["paper_shadow_daily"]["observation_id"],
        paper_shadow_drift_monitor_id=fixture["paper_shadow_drift"]["monitor_id"],
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        paper_shadow_daily_dir=tmp_path / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=tmp_path / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=tmp_path / "paper_shadow_weekly_review",
        output_dir=tmp_path / "evidence_staleness_monitor_latest_weekly",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    report = result["evidence_staleness_report"]
    findings = {row["source_id"]: row for row in report["findings"]}

    assert "paper_shadow_weekly_review" not in report["missing_artifacts"]
    assert "paper_shadow_weekly_review" not in report["blocking_artifacts"]
    assert report["safe_to_continue_shadow"] is True
    assert findings["paper_shadow_weekly_review"]["artifact_id"] == fixture[
        "paper_shadow_weekly"
    ]["weekly_review_id"]
    assert findings["paper_shadow_weekly_review"]["missing"] is False


def _paper_shadow_freshness_fixture(tmp_path: Path) -> dict[str, object]:
    fixture = run_paper_shadow_protocol_fixture(tmp_path)
    ledger = readiness.record_candidate_decision_ledger(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        evidence_id=fixture["filtered_candidate_evidence"]["evidence_id"],
        stress_backfill_id=fixture["filtered_candidate_stress_backfill"]["stress_backfill_id"],
        mismatch_reduction_id=fixture["drawdown_mismatch_reduction"]["reduction_id"],
        flip_reduction_id=fixture["flip_rotation_reduction"]["flip_reduction_id"],
        ab_review_id=fixture["filtered_candidate_ab_review"]["ab_review_id"],
        confirmation_id=fixture["signal_gate_confirmation"]["confirmation_id"],
        owner_review_id=fixture["owner_filtered_candidate_review"]["owner_review_id"],
        next_decision_id=fixture["filtered_next_decision"]["decision_id"],
        contract_id=fixture["formal_research_method_contract"]["contract_id"],
        protocol_id=fixture["paper_shadow_protocol"]["protocol_id"],
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        mismatch_reduction_dir=tmp_path / "drawdown_mismatch_reduction",
        flip_reduction_dir=tmp_path / "flip_rotation_reduction",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        confirmation_dir=tmp_path / "signal_gate_confirmation",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        next_decision_dir=tmp_path / "filtered_next_decision",
        contract_dir=tmp_path / "formal_research_method_contract",
        protocol_dir=tmp_path / "paper_shadow_protocol",
        output_dir=tmp_path / "candidate_decision_ledger",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    market_panel = tmp_path / "paper_shadow_market_panel_2024-04-22.json"
    market_panel.write_text(json.dumps({"as_of": "2024-04-22"}), encoding="utf-8")
    signal_artifact = tmp_path / "paper_shadow_signal_2024-04-22.json"
    signal_artifact.write_text(
        json.dumps({"signal_output": "OBSERVE_RISK_ON"}),
        encoding="utf-8",
    )
    observation = daily.run_paper_shadow_daily_observation(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        observation_date="2024-04-22",
        market_panel_artifact=market_panel,
        signal_artifact=signal_artifact,
        signal_output="OBSERVE_RISK_ON",
        hypothetical_weight_recommendation="paper_shadow_only_no_official_weight",
        risk_off_risk_on_state="risk_on",
        drawdown_state="normal",
        rotation_event="none",
        mismatch_event="none",
        benchmark_comparison="tracking_QQQ_SPY_SMH",
        manual_reviewer_notes="synthetic staleness monitor fixture",
        contract_id=fixture["formal_research_method_contract"]["contract_id"],
        protocol_id=fixture["paper_shadow_protocol"]["protocol_id"],
        contract_dir=tmp_path / "formal_research_method_contract",
        protocol_dir=tmp_path / "paper_shadow_protocol",
        output_dir=tmp_path / "paper_shadow_daily",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    drift_monitor = drift.build_paper_shadow_drift_monitor_report(
        observation_id=observation["observation_id"],
        observation_dir=tmp_path / "paper_shadow_daily",
        contract_id=fixture["formal_research_method_contract"]["contract_id"],
        contract_dir=tmp_path / "formal_research_method_contract",
        output_dir=tmp_path / "paper_shadow_drift_monitor",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    weekly_review = weekly.build_paper_shadow_weekly_review(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        week_start="2024-04-22",
        week_end="2024-04-22",
        daily_observation_ids=[observation["observation_id"]],
        drift_monitor_ids=[drift_monitor["monitor_id"]],
        contract_id=fixture["formal_research_method_contract"]["contract_id"],
        ledger_run_id=ledger["ledger_run_id"],
        observation_dir=tmp_path / "paper_shadow_daily",
        drift_dir=tmp_path / "paper_shadow_drift_monitor",
        contract_dir=tmp_path / "formal_research_method_contract",
        ledger_dir=tmp_path / "candidate_decision_ledger",
        output_dir=tmp_path / "paper_shadow_weekly_review",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    return {
        **fixture,
        "candidate_decision_ledger": ledger,
        "paper_shadow_daily": observation,
        "paper_shadow_drift": drift_monitor,
        "paper_shadow_weekly": weekly_review,
    }
