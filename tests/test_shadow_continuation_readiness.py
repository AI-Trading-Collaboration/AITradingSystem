from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_paper_shadow_protocol_fixture,
    run_signal_input_completeness_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_daily as daily
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_drift as drift
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.platform.artifacts.validation_session import (
    artifact_validation_session,
)


@pytest.fixture(autouse=True)
def _reuse_validated_artifacts_within_test():
    with artifact_validation_session():
        yield


@pytest.fixture(scope="module")
def shared_shadow_continuation_fixture(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[dict[str, object]]:
    root = tmp_path_factory.mktemp("shadow-continuation-source-fixture")
    monkeypatch = pytest.MonkeyPatch()
    try:
        with artifact_validation_session():
            fixture = _shadow_continuation_fixture(root, monkeypatch)
        fixture["fixture_root"] = root
        yield fixture
    finally:
        monkeypatch.undo()


def test_shadow_continuation_readiness_requires_manual_review_for_recovery_week(
    tmp_path: Path, shared_shadow_continuation_fixture: dict[str, object]
) -> None:
    fixture = shared_shadow_continuation_fixture
    fixture_root = Path(fixture["fixture_root"])
    data_quality_report = _write_data_quality_report(tmp_path)

    result = readiness.run_shadow_continuation_readiness_report(
        as_of=date(2024, 4, 22),
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        paper_shadow_daily_id=fixture["paper_shadow_daily"]["observation_id"],
        paper_shadow_drift_monitor_id=fixture["paper_shadow_drift"]["monitor_id"],
        paper_shadow_weekly_review_id=fixture["paper_shadow_weekly"]["weekly_review_id"],
        evidence_staleness_monitor_id=fixture["evidence_staleness"]["monitor_id"],
        signal_input_completeness_id=fixture["signal_input_completeness"]["monitor_id"],
        data_quality_report_path=data_quality_report,
        paper_shadow_daily_dir=fixture_root / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=fixture_root / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=fixture_root / "paper_shadow_weekly_review",
        evidence_staleness_monitor_dir=fixture_root / "evidence_staleness_monitor",
        signal_input_completeness_dir=fixture_root / "signal_input_completeness",
        output_dir=tmp_path / "shadow_continuation_readiness",
        generated_at=datetime(2024, 4, 22, 1, tzinfo=UTC),
    )
    report = result["shadow_continuation_readiness_report"]
    validation = readiness.validate_shadow_continuation_readiness_artifact(
        readiness_id=result["readiness_id"],
        output_dir=tmp_path / "shadow_continuation_readiness",
    )
    payload = readiness.shadow_continuation_readiness_report_payload(
        readiness_id=result["readiness_id"],
        output_dir=tmp_path / "shadow_continuation_readiness",
    )

    assert validation["status"] == "PASS"
    assert payload["shadow_continuation_readiness_report"]["readiness_id"] == result["readiness_id"]
    assert report["shadow_continuation_readiness"] == "MANUAL_REVIEW_REQUIRED"
    assert report["safe_to_continue_shadow"] is False
    assert report["missing_artifacts"] == []
    assert report["blocking_artifacts"] == []
    assert report["stale_artifacts"] == []
    assert report["coverage_status"] == "MANUAL_REVIEW_REQUIRED"
    assert report["manual_review_required"] is True
    assert (
        report["next_required_action"]
        == "complete_full_weekly_review_or_record_manual_coverage_override"
    )
    assert report["data_validation_status"] == "PASS_WITH_WARNINGS"
    assert report["data_validation_result"]["warning_count"] == 1
    assert report["safety_boundary_status"] == "PASS"
    assert report["source_artifacts"]["paper_shadow_weekly_review"]["exists"] is True
    assert "shadow_continuation_readiness" in result["reader_brief_section"]
    assert result["manifest"]["data_downloaded_by_readiness"] is False
    assert result["manifest"]["pipelines_executed_by_readiness"] is False
    assert_research_safe(result["manifest"])


def test_shadow_continuation_readiness_blocks_missing_weekly_review(
    tmp_path: Path, shared_shadow_continuation_fixture: dict[str, object]
) -> None:
    fixture = shared_shadow_continuation_fixture
    fixture_root = Path(fixture["fixture_root"])
    data_quality_report = _write_data_quality_report(tmp_path, status="PASS")

    result = readiness.run_shadow_continuation_readiness_report(
        as_of=date(2024, 4, 22),
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        paper_shadow_daily_id=fixture["paper_shadow_daily"]["observation_id"],
        paper_shadow_drift_monitor_id=fixture["paper_shadow_drift"]["monitor_id"],
        paper_shadow_weekly_review_id="missing-weekly-review",
        evidence_staleness_monitor_id=fixture["evidence_staleness"]["monitor_id"],
        signal_input_completeness_id=fixture["signal_input_completeness"]["monitor_id"],
        data_quality_report_path=data_quality_report,
        paper_shadow_daily_dir=fixture_root / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=fixture_root / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=fixture_root / "paper_shadow_weekly_review",
        evidence_staleness_monitor_dir=fixture_root / "evidence_staleness_monitor",
        signal_input_completeness_dir=fixture_root / "signal_input_completeness",
        output_dir=tmp_path / "shadow_continuation_readiness_missing",
        generated_at=datetime(2024, 4, 22, 1, tzinfo=UTC),
    )
    report = result["shadow_continuation_readiness_report"]

    assert report["shadow_continuation_readiness"] == "BLOCKED_MISSING_ARTIFACTS"
    assert report["safe_to_continue_shadow"] is False
    assert report["missing_artifacts"] == ["paper_shadow_weekly_review"]
    assert report["source_artifacts"]["paper_shadow_weekly_review"]["exists"] is False
    assert result["shadow_continuation_readiness_validation"]["status"] == "PASS"


def test_shadow_continuation_readiness_blocks_fallback_unavailable(
    tmp_path: Path, shared_shadow_continuation_fixture: dict[str, object]
) -> None:
    fixture = shared_shadow_continuation_fixture
    fixture_root = Path(fixture["fixture_root"])
    data_quality_report = _write_data_quality_report(tmp_path, status="PASS")
    fallback_policy_report = _write_fallback_policy_report(
        tmp_path,
        fallback_status="FALLBACK_UNAVAILABLE",
        status="FAIL",
        blocking_data_types=["price_data"],
    )

    result = readiness.run_shadow_continuation_readiness_report(
        as_of=date(2024, 4, 22),
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        paper_shadow_daily_id=fixture["paper_shadow_daily"]["observation_id"],
        paper_shadow_drift_monitor_id=fixture["paper_shadow_drift"]["monitor_id"],
        paper_shadow_weekly_review_id=fixture["paper_shadow_weekly"]["weekly_review_id"],
        evidence_staleness_monitor_id=fixture["evidence_staleness"]["monitor_id"],
        signal_input_completeness_id=fixture["signal_input_completeness"]["monitor_id"],
        data_quality_report_path=data_quality_report,
        paper_shadow_daily_dir=fixture_root / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=fixture_root / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=fixture_root / "paper_shadow_weekly_review",
        evidence_staleness_monitor_dir=fixture_root / "evidence_staleness_monitor",
        signal_input_completeness_dir=fixture_root / "signal_input_completeness",
        fallback_policy_report_path=fallback_policy_report,
        output_dir=tmp_path / "shadow_continuation_readiness_fallback_blocked",
        generated_at=datetime(2024, 4, 22, 1, tzinfo=UTC),
    )
    report = result["shadow_continuation_readiness_report"]

    assert report["shadow_continuation_readiness"] == "BLOCKED_STALE_DATA"
    assert report["safe_to_continue_shadow"] is False
    assert "data_source_fallback_policy" in report["blocking_artifacts"]
    assert report["fallback_status"] == "FALLBACK_UNAVAILABLE"
    assert report["fallback_blocking_data_types"] == "price_data"
    assert result["shadow_continuation_readiness_validation"]["status"] == "PASS"


def test_shadow_continuation_readiness_blocks_cache_catalog_failure(
    tmp_path: Path, shared_shadow_continuation_fixture: dict[str, object]
) -> None:
    fixture = shared_shadow_continuation_fixture
    fixture_root = Path(fixture["fixture_root"])
    data_quality_report = _write_data_quality_report(tmp_path, status="PASS")
    cache_catalog_report = _write_cache_catalog_report(tmp_path, status="FAIL")

    result = readiness.run_shadow_continuation_readiness_report(
        as_of=date(2024, 4, 22),
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        paper_shadow_daily_id=fixture["paper_shadow_daily"]["observation_id"],
        paper_shadow_drift_monitor_id=fixture["paper_shadow_drift"]["monitor_id"],
        paper_shadow_weekly_review_id=fixture["paper_shadow_weekly"]["weekly_review_id"],
        evidence_staleness_monitor_id=fixture["evidence_staleness"]["monitor_id"],
        signal_input_completeness_id=fixture["signal_input_completeness"]["monitor_id"],
        data_quality_report_path=data_quality_report,
        paper_shadow_daily_dir=fixture_root / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=fixture_root / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=fixture_root / "paper_shadow_weekly_review",
        evidence_staleness_monitor_dir=fixture_root / "evidence_staleness_monitor",
        signal_input_completeness_dir=fixture_root / "signal_input_completeness",
        cache_catalog_report_path=cache_catalog_report,
        output_dir=tmp_path / "shadow_continuation_readiness_cache_blocked",
        generated_at=datetime(2024, 4, 22, 1, tzinfo=UTC),
    )
    report = result["shadow_continuation_readiness_report"]

    assert report["shadow_continuation_readiness"] == "BLOCKED_STALE_DATA"
    assert report["safe_to_continue_shadow"] is False
    assert "cache_catalog" in report["blocking_artifacts"]
    assert report["cache_integrity_status"] == "FAIL"
    assert report["cache_checksum_mismatch_count"] == 1
    assert result["shadow_continuation_readiness_validation"]["status"] == "PASS"


def test_shadow_continuation_readiness_cli_run_report_and_validate(
    tmp_path: Path, shared_shadow_continuation_fixture: dict[str, object]
) -> None:
    fixture = shared_shadow_continuation_fixture
    fixture_root = Path(fixture["fixture_root"])
    data_quality_report = _write_data_quality_report(tmp_path)
    output_dir = tmp_path / "shadow_continuation_readiness_cli"

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "shadow-continuation-readiness",
            "run",
            "--as-of",
            "2024-04-22",
            "--candidate",
            readiness.TOP_FILTERED_CANDIDATE,
            "--paper-shadow-daily-id",
            fixture["paper_shadow_daily"]["observation_id"],
            "--paper-shadow-drift-monitor-id",
            fixture["paper_shadow_drift"]["monitor_id"],
            "--paper-shadow-weekly-review-id",
            fixture["paper_shadow_weekly"]["weekly_review_id"],
            "--evidence-staleness-monitor-id",
            fixture["evidence_staleness"]["monitor_id"],
            "--signal-input-completeness-id",
            fixture["signal_input_completeness"]["monitor_id"],
            "--data-quality-report-path",
            str(data_quality_report),
            "--paper-shadow-daily-dir",
            str(fixture_root / "paper_shadow_daily"),
            "--paper-shadow-drift-monitor-dir",
            str(fixture_root / "paper_shadow_drift_monitor"),
            "--paper-shadow-weekly-review-dir",
            str(fixture_root / "paper_shadow_weekly_review"),
            "--evidence-staleness-monitor-dir",
            str(fixture_root / "evidence_staleness_monitor"),
            "--signal-input-completeness-dir",
            str(fixture_root / "signal_input_completeness"),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    assert "shadow_continuation_readiness=MANUAL_REVIEW_REQUIRED" in run.output
    assert "safe_to_continue_shadow=False" in run.output
    assert "validation_status=PASS" in run.output
    readiness_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("readiness_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "shadow-continuation-readiness",
            "report",
            "--readiness-id",
            readiness_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "data_validation_status=PASS_WITH_WARNINGS" in report.output
    assert "safety_boundary_status=PASS" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-shadow-continuation-readiness",
            "--readiness-id",
            readiness_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def _shadow_continuation_fixture(tmp_path: Path, monkeypatch) -> dict[str, object]:
    fixture = run_paper_shadow_protocol_fixture(
        tmp_path,
        monkeypatch,
        evidence_date_end="2024-04-19",
    )
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
        manual_reviewer_notes="synthetic shadow continuation fixture",
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
    price_cache_path = tmp_path / "prices_daily.csv"
    price_cache_path.write_text(
        "date,ticker,close\n2024-04-19,QQQ,431\n",
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
    staleness = readiness.run_evidence_staleness_monitor(
        as_of=date(2024, 4, 22),
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        price_cache_path=price_cache_path,
        market_panel_dir=market_panel_dir,
        evidence_id=fixture["filtered_candidate_evidence"]["evidence_id"],
        stress_backfill_id=fixture["filtered_candidate_stress_backfill"]["stress_backfill_id"],
        ab_review_id=fixture["filtered_candidate_ab_review"]["ab_review_id"],
        owner_review_id=fixture["owner_filtered_candidate_review"]["owner_review_id"],
        paper_shadow_daily_id=observation["observation_id"],
        paper_shadow_drift_monitor_id=drift_monitor["monitor_id"],
        paper_shadow_weekly_review_id=weekly_review["weekly_review_id"],
        signal_input_completeness_id=signal_input["monitor_id"],
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        paper_shadow_daily_dir=tmp_path / "paper_shadow_daily",
        paper_shadow_drift_monitor_dir=tmp_path / "paper_shadow_drift_monitor",
        paper_shadow_weekly_review_dir=tmp_path / "paper_shadow_weekly_review",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
        output_dir=tmp_path / "evidence_staleness_monitor",
        generated_at=datetime(2024, 4, 22, tzinfo=UTC),
    )
    return {
        **fixture,
        "candidate_decision_ledger": ledger,
        "paper_shadow_daily": observation,
        "paper_shadow_drift": drift_monitor,
        "paper_shadow_weekly": weekly_review,
        "evidence_staleness": staleness,
        "signal_input_completeness": signal_input,
    }


def _write_data_quality_report(tmp_path: Path, *, status: str = "PASS_WITH_WARNINGS") -> Path:
    report_dir = tmp_path / "data_quality_reports"
    report_dir.mkdir()
    warning_count = 1 if status == "PASS_WITH_WARNINGS" else 0
    path = report_dir / "data_quality_2024-04-19.md"
    path.write_text(
        "\n".join(
            [
                "# 数据质量报告",
                "",
                f"- 状态：{status}",
                "- 检查时间：2024-04-22T01:00:00+00:00",
                "- 评估日期：2024-04-19",
                "- 错误数：0",
                f"- 警告数：{warning_count}",
                "- 信息数：2",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _write_fallback_policy_report(
    tmp_path: Path,
    *,
    fallback_status: str,
    status: str,
    blocking_data_types: list[str],
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
                    "fallback_used_count": 0,
                    "fallback_unavailable_count": 1
                    if fallback_status == "FALLBACK_UNAVAILABLE"
                    else 0,
                    "blocked_no_valid_source_count": 0,
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
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return report_path
