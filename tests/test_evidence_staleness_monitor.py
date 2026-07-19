from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_paper_shadow_protocol_fixture,
    run_signal_input_completeness_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_daily as daily
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_drift as drift
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly


def test_evidence_staleness_monitor_builds_and_validates(tmp_path: Path, monkeypatch) -> None:
    fixture = _paper_shadow_freshness_fixture(tmp_path, monkeypatch)
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
    cache_catalog_report = _write_cache_catalog_report(tmp_path, status="PASS")

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
        signal_input_completeness_id=fixture["signal_input_completeness"]["monitor_id"],
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        paper_shadow_daily_dir=tmp_path / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=tmp_path / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=tmp_path / "paper_shadow_weekly_review",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
        cache_catalog_report_path=cache_catalog_report,
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
    assert report["evidence_freshness_status"] == "FRESH"
    assert report["requested_as_of"] == "2024-04-22"
    assert report["freshness_reference_date"] == "2024-04-19"
    assert report["latest_complete_market_date"] == "2024-04-19"
    assert report["market_calendar_status"] == "TRADING_DAY"
    assert report["market_session_kind"] == "NORMAL_TRADING_DAY"
    assert report["calendar_adjustment_reason"] == (
        "requested_as_of_after_latest_complete_market_date"
    )
    assert report["calendar_adjusted_staleness"] is True
    assert report["stale_artifacts"] == []
    assert report["blocking_artifacts"] == []
    assert report["missing_artifacts"] == []
    assert report["coverage_status"] == "MANUAL_REVIEW_REQUIRED"
    assert report["coverage_blocking_artifacts"] == ["paper_shadow_weekly_review"]
    assert report["weekly_review_coverage_classification"] == "RECOVERY_MODE_REVIEW"
    assert report["weekly_review_coverage_safe_for_continuation"] is False
    assert report["cache_integrity_status"] == "OK"
    assert report["cache_checksum_mismatch_count"] == 0
    assert (
        report["next_refresh_action"]
        == "complete_full_weekly_review_or_record_manual_coverage_override"
    )
    assert report["safe_to_continue_shadow"] is False
    assert report["safety_boundary_status"] == "PASS"
    assert findings["price_data"]["severity"] == "FRESH"
    assert findings["price_data"]["requested_as_of"] == "2024-04-22"
    assert findings["price_data"]["freshness_reference_date"] == "2024-04-19"
    assert findings["price_data"]["calendar_adjusted_staleness"] is True
    assert findings["price_data"]["calendar_adjustment_reason"] == (
        "requested_as_of_after_latest_complete_market_date"
    )
    assert findings["price_data"]["stale_reason"] == (
        "calendar_adjusted_to_latest_complete_market_date"
    )
    assert findings["market_panel_data"]["severity"] == "FRESH"
    assert findings["market_panel_data"]["freshness_reference_date"] == "2024-04-19"
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
    assert findings["paper_shadow_weekly_review"]["coverage_classification"] == (
        "RECOVERY_MODE_REVIEW"
    )
    assert findings["paper_shadow_weekly_review"]["coverage_safe_for_continuation"] is False
    assert "evidence_freshness_status" in result["reader_brief_section"]
    assert "missing_artifacts" in result["reader_brief_section"]
    assert "weekly_review_coverage_classification" in result["reader_brief_section"]
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


def test_evidence_staleness_weekend_market_reference_does_not_hide_real_stale_data() -> None:
    calendar_context = readiness._market_data_freshness_context(
        requested_as_of=date(2024, 4, 20),
        generated_at=datetime(2024, 4, 22, 16, 0, tzinfo=UTC),
    )
    fresh_finding = readiness._evidence_freshness_finding(
        source_id="price_data",
        source_label="Price data",
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        timestamp=date(2024, 4, 19),
        timestamp_basis="latest_price_cache_date",
        source_path=None,
        artifact_id="prices_daily.csv",
        rule={"fresh_days": 1, "acceptable_days": 3, "blocking_days": 7},
        as_of=date(2024, 4, 19),
        requested_as_of=date(2024, 4, 20),
        market_calendar=calendar_context,
    )
    stale_finding = readiness._evidence_freshness_finding(
        source_id="price_data",
        source_label="Price data",
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        timestamp=date(2024, 4, 10),
        timestamp_basis="latest_price_cache_date",
        source_path=None,
        artifact_id="prices_daily.csv",
        rule={"fresh_days": 1, "acceptable_days": 3, "blocking_days": 7},
        as_of=date(2024, 4, 19),
        requested_as_of=date(2024, 4, 20),
        market_calendar=calendar_context,
    )

    assert calendar_context["freshness_reference_date"] == "2024-04-19"
    assert calendar_context["market_session_kind"] == "WEEKEND"
    assert fresh_finding["severity"] == "FRESH"
    assert fresh_finding["calendar_adjusted_staleness"] is True
    assert fresh_finding["stale_reason"] == "calendar_adjusted_to_latest_complete_market_date"
    assert stale_finding["severity"] == "BLOCKING"
    assert stale_finding["stale_reason"] == "older_than_blocking_policy_window"


def test_evidence_staleness_missing_weekly_review_blocks(tmp_path: Path, monkeypatch) -> None:
    fixture = _paper_shadow_freshness_fixture(tmp_path, monkeypatch)
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
        signal_input_completeness_id=fixture["signal_input_completeness"]["monitor_id"],
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        paper_shadow_daily_dir=tmp_path / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=tmp_path / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=tmp_path / "paper_shadow_weekly_review",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
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


def test_evidence_staleness_discovers_latest_weekly_review_artifact(
    tmp_path: Path, monkeypatch
) -> None:
    fixture = _paper_shadow_freshness_fixture(tmp_path, monkeypatch)
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
        signal_input_completeness_id=fixture["signal_input_completeness"]["monitor_id"],
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        paper_shadow_daily_dir=tmp_path / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=tmp_path / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=tmp_path / "paper_shadow_weekly_review",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
        output_dir=tmp_path / "evidence_staleness_monitor_latest_weekly",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    report = result["evidence_staleness_report"]
    findings = {row["source_id"]: row for row in report["findings"]}

    assert "paper_shadow_weekly_review" not in report["missing_artifacts"]
    assert "paper_shadow_weekly_review" not in report["blocking_artifacts"]
    assert report["safe_to_continue_shadow"] is False
    assert report["coverage_status"] == "MANUAL_REVIEW_REQUIRED"
    assert (
        findings["paper_shadow_weekly_review"]["artifact_id"]
        == fixture["paper_shadow_weekly"]["weekly_review_id"]
    )
    assert findings["paper_shadow_weekly_review"]["missing"] is False
    assert findings["paper_shadow_weekly_review"]["coverage_classification"] == (
        "RECOVERY_MODE_REVIEW"
    )


def test_evidence_staleness_blocks_on_fallback_policy_blocker(tmp_path: Path, monkeypatch) -> None:
    fixture = _paper_shadow_freshness_fixture(tmp_path, monkeypatch)
    fallback_policy_report = _write_fallback_policy_report(
        tmp_path,
        fallback_status="BLOCKED_NO_VALID_SOURCE",
        status="FAIL",
        blocking_data_types=["price_data"],
        fallback_used_count=0,
    )
    evidence_manifest_path = Path(
        fixture["filtered_candidate_evidence"]["manifest"][
            "filtered_candidate_evidence_manifest_path"
        ]
    )
    evidence_manifest = json.loads(evidence_manifest_path.read_text(encoding="utf-8"))
    evidence_manifest["date_end"] = "2024-04-19"
    evidence_manifest_path.write_text(
        json.dumps(evidence_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    price_cache_path = tmp_path / "prices_daily.csv"
    price_cache_path.write_text(
        "date,ticker,close\n2024-04-19,QQQ,431\n",
        encoding="utf-8",
    )
    market_panel_dir = tmp_path / "market_panel"
    market_panel_dir.mkdir()
    (market_panel_dir / "market_panel_2024-04-19.json").write_text(
        json.dumps({"status": "PASS", "as_of": "2024-04-19"}),
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
        signal_input_completeness_id=fixture["signal_input_completeness"]["monitor_id"],
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        paper_shadow_daily_dir=tmp_path / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=tmp_path / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=tmp_path / "paper_shadow_weekly_review",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
        fallback_policy_report_path=fallback_policy_report,
        output_dir=tmp_path / "evidence_staleness_monitor_fallback_blocked",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    report = result["evidence_staleness_report"]

    assert report["evidence_freshness_status"] == "BLOCKING"
    assert report["safe_to_continue_shadow"] is False
    assert "data_source_fallback_policy" in report["blocking_artifacts"]
    assert report["fallback_status"] == "BLOCKED_NO_VALID_SOURCE"
    assert report["fallback_blocking_data_types"] == "price_data"
    assert report["next_refresh_action"] == "restore_primary_or_valid_fallback_source"
    assert result["evidence_staleness_validation"]["status"] == "PASS"


def _paper_shadow_freshness_fixture(tmp_path: Path, monkeypatch) -> dict[str, object]:
    fixture = run_paper_shadow_protocol_fixture(tmp_path, monkeypatch)
    signal_input = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
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
        signal_input_completeness_id=signal_input["monitor_id"],
        contract_dir=tmp_path / "formal_research_method_contract",
        protocol_dir=tmp_path / "paper_shadow_protocol",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
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
        signal_input_completeness_id=signal_input["monitor_id"],
        observation_dir=tmp_path / "paper_shadow_daily",
        drift_dir=tmp_path / "paper_shadow_drift_monitor",
        contract_dir=tmp_path / "formal_research_method_contract",
        ledger_dir=tmp_path / "candidate_decision_ledger",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
        output_dir=tmp_path / "paper_shadow_weekly_review",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    return {
        **fixture,
        "candidate_decision_ledger": ledger,
        "paper_shadow_daily": observation,
        "paper_shadow_drift": drift_monitor,
        "paper_shadow_weekly": weekly_review,
        "signal_input_completeness": signal_input,
    }


def _write_fallback_policy_report(
    tmp_path: Path,
    *,
    fallback_status: str,
    status: str,
    blocking_data_types: list[str],
    fallback_used_count: int,
) -> Path:
    report_path = tmp_path / f"fallback_policy_{fallback_status}.json"
    report_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "data_source_fallback_policy",
                "report_id": f"fallback-policy-{fallback_status}",
                "as_of": "2024-04-22",
                "status": status,
                "validation_status": status,
                "production_effect": "none",
                "safety_boundary": {
                    "read_only": True,
                    "data_refresh_allowed": False,
                    "cache_mutation_allowed": False,
                    "score_or_backtest_allowed": False,
                    "broker_action_allowed": False,
                    "order_ticket_allowed": False,
                    "production_state_mutation_allowed": False,
                },
                "summary": {
                    "fallback_status": fallback_status,
                    "source_group_count": 1,
                    "primary_ok_count": 0,
                    "fallback_used_count": fallback_used_count,
                    "fallback_unavailable_count": 0,
                    "blocked_no_valid_source_count": 1
                    if fallback_status == "BLOCKED_NO_VALID_SOURCE"
                    else 0,
                    "blocking_source_count": len(blocking_data_types),
                    "fallback_used_sources": [],
                    "blocking_data_types": blocking_data_types,
                    "next_action": "restore_primary_or_valid_fallback_source",
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return report_path


def _write_cache_catalog_report(tmp_path: Path, *, status: str) -> Path:
    report_path = tmp_path / f"cache_catalog_{status}.json"
    integrity = "OK" if status == "PASS" else "FAIL"
    report_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "cache_catalog",
                "catalog_id": f"cache-catalog-{status}",
                "as_of": "2024-04-22",
                "status": status,
                "validation_status": status,
                "cache_integrity_status": integrity,
                "production_effect": "none",
                "summary": {
                    "cache_integrity_status": integrity,
                    "entry_count": 4,
                    "required_entry_count": 4,
                    "missing_required_count": 0 if status == "PASS" else 1,
                    "missing_optional_count": 0,
                    "checksum_mismatch_count": 0 if status == "PASS" else 1,
                    "checksum_changed_without_refresh_count": 0,
                    "blocking_entry_count": 0 if status == "PASS" else 1,
                    "blocking_entry_ids": [] if status == "PASS" else ["primary_price_cache"],
                    "warning_entry_ids": [],
                    "refresh_audit_id": "data_refresh_audit_test",
                    "validated_at": "2024-04-22T10:01:00+00:00",
                    "next_action": (
                        "repair_cache_lineage_then_rerun_validate_data_and_cache_catalog"
                    ),
                },
                "safety_boundary": {
                    "read_only": True,
                    "data_refresh_allowed": False,
                    "cache_mutation_allowed": False,
                    "cache_repair_allowed": False,
                    "score_or_backtest_allowed": False,
                    "broker_action_allowed": False,
                    "order_ticket_allowed": False,
                    "production_state_mutation_allowed": False,
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return report_path
