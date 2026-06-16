from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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


def test_paper_shadow_weekly_review_builds_and_validates(tmp_path: Path) -> None:
    fixture = _weekly_fixture(tmp_path)
    result = weekly.build_paper_shadow_weekly_review(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        week_start="2026-06-08",
        week_end="2026-06-12",
        daily_observation_ids=fixture["daily_ids"],
        drift_monitor_ids=fixture["drift_ids"],
        contract_id=fixture["contract_id"],
        ledger_run_id=fixture["ledger_run_id"],
        signal_input_completeness_id=fixture["signal_input_completeness_id"],
        observation_dir=tmp_path / "paper_shadow_daily",
        drift_dir=tmp_path / "paper_shadow_drift_monitor",
        contract_dir=tmp_path / "formal_research_method_contract",
        ledger_dir=tmp_path / "candidate_decision_ledger",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
        output_dir=tmp_path / "paper_shadow_weekly_review",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    review = result["paper_shadow_weekly_review"]
    validation = result["paper_shadow_weekly_validation"]
    payload = weekly.paper_shadow_weekly_review_report_payload(
        weekly_review_id=result["weekly_review_id"],
        output_dir=tmp_path / "paper_shadow_weekly_review",
    )

    assert review["weekly_decision"] == "CONTINUE"
    assert review["signal_input_status"] == "OK"
    assert review["coverage_classification"] == "FULL_WEEK_REVIEW"
    assert review["coverage_safe_for_continuation"] is True
    assert review["expected_market_days"] == [
        "2026-06-08",
        "2026-06-09",
        "2026-06-10",
        "2026-06-11",
        "2026-06-12",
    ]
    assert review["covered_market_days"] == review["expected_market_days"]
    assert review["missing_market_days"] == []
    assert review["coverage_ratio"] == 1.0
    assert review["summary"]["signal_stability"] == "STABLE"
    assert review["summary"]["missing_input_artifacts"] == []
    assert validation["status"] == "PASS"
    assert payload["paper_shadow_weekly_review"]["weekly_decision"] == "CONTINUE"
    assert "paper_shadow_weekly_review_id" in result["reader_brief_section"]
    assert_research_safe(review)
    assert review["paper_account_state_mutated"] is False
    assert review["data_downloaded_by_review"] is False


def test_paper_shadow_weekly_review_discloses_missing_daily_input(
    tmp_path: Path,
) -> None:
    fixture = _weekly_fixture(tmp_path, missing_market_panel=True)
    result = weekly.build_paper_shadow_weekly_review(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        week_start="2026-06-08",
        week_end="2026-06-12",
        daily_observation_ids=fixture["daily_ids"],
        drift_monitor_ids=fixture["drift_ids"],
        contract_id=fixture["contract_id"],
        ledger_run_id=fixture["ledger_run_id"],
        signal_input_completeness_id=fixture["signal_input_completeness_id"],
        observation_dir=tmp_path / "paper_shadow_daily",
        drift_dir=tmp_path / "paper_shadow_drift_monitor",
        contract_dir=tmp_path / "formal_research_method_contract",
        ledger_dir=tmp_path / "candidate_decision_ledger",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
        output_dir=tmp_path / "paper_shadow_weekly_review",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    review = result["paper_shadow_weekly_review"]

    assert review["weekly_decision"] == "RETURN_TO_RESEARCH"
    assert any(
        "market_panel_artifact" in item
        for item in review["summary"]["missing_input_artifacts"]
    )
    assert result["paper_shadow_weekly_validation"]["status"] == "PASS"


def test_paper_shadow_weekly_cli_build_report_and_validate(tmp_path: Path) -> None:
    fixture = _weekly_fixture(tmp_path)
    output_dir = tmp_path / "paper_shadow_weekly_review"
    source_args: list[str] = []
    for daily_id in fixture["daily_ids"]:
        source_args.extend(["--daily-observation-id", daily_id])
    for drift_id in fixture["drift_ids"]:
        source_args.extend(["--drift-monitor-id", drift_id])
    result = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "paper-shadow-weekly-review",
            "build",
            "--candidate",
            readiness.TOP_FILTERED_CANDIDATE,
            "--week-start",
            "2026-06-08",
            "--week-end",
            "2026-06-12",
            *source_args,
            "--contract-id",
            fixture["contract_id"],
            "--ledger-run-id",
            fixture["ledger_run_id"],
            "--signal-input-completeness-id",
            fixture["signal_input_completeness_id"],
            "--observation-dir",
            str(tmp_path / "paper_shadow_daily"),
            "--drift-dir",
            str(tmp_path / "paper_shadow_drift_monitor"),
            "--contract-dir",
            str(tmp_path / "formal_research_method_contract"),
            "--ledger-dir",
            str(tmp_path / "candidate_decision_ledger"),
            "--signal-input-completeness-dir",
            str(tmp_path / "signal_input_completeness"),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    assert "weekly_decision=CONTINUE" in result.output
    assert "signal_input_status=OK" in result.output
    assert "coverage_classification=FULL_WEEK_REVIEW" in result.output
    assert "coverage_safe_for_continuation=True" in result.output
    weekly_review_id = next(
        line.split("=", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("weekly_review_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "paper-shadow-weekly-review",
            "report",
            "--weekly-review-id",
            weekly_review_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "validation_status=PASS" in report.output
    assert "coverage_status=PASS" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-paper-shadow-weekly-review",
            "--weekly-review-id",
            weekly_review_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def test_paper_shadow_weekly_validation_rejects_illegal_decision(
    tmp_path: Path,
) -> None:
    fixture = _weekly_fixture(tmp_path)
    result = weekly.build_paper_shadow_weekly_review(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        week_start="2026-06-08",
        week_end="2026-06-12",
        daily_observation_ids=fixture["daily_ids"],
        drift_monitor_ids=fixture["drift_ids"],
        contract_id=fixture["contract_id"],
        ledger_run_id=fixture["ledger_run_id"],
        signal_input_completeness_id=fixture["signal_input_completeness_id"],
        observation_dir=tmp_path / "paper_shadow_daily",
        drift_dir=tmp_path / "paper_shadow_drift_monitor",
        contract_dir=tmp_path / "formal_research_method_contract",
        ledger_dir=tmp_path / "candidate_decision_ledger",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
        output_dir=tmp_path / "paper_shadow_weekly_review",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    review_path = (
        tmp_path
        / "paper_shadow_weekly_review"
        / result["weekly_review_id"]
        / "paper_shadow_weekly_review.json"
    )
    review = json.loads(review_path.read_text(encoding="utf-8"))
    review["weekly_decision"] = "PROMOTE"
    review_path.write_text(
        json.dumps(review, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    validation = weekly.validate_paper_shadow_weekly_review_artifact(
        weekly_review_id=result["weekly_review_id"],
        output_dir=tmp_path / "paper_shadow_weekly_review",
    )
    failed = {row["check_id"] for row in validation["checks"] if row["passed"] is False}

    assert validation["status"] == "FAIL"
    assert "weekly_decision_valid" in failed


def test_paper_shadow_weekly_recovery_window_is_not_full_week(
    tmp_path: Path,
) -> None:
    fixture = _weekly_fixture(tmp_path, days=("2026-06-12",))
    result = weekly.build_paper_shadow_weekly_review(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        week_start="2026-06-12",
        week_end="2026-06-12",
        daily_observation_ids=fixture["daily_ids"],
        drift_monitor_ids=fixture["drift_ids"],
        contract_id=fixture["contract_id"],
        ledger_run_id=fixture["ledger_run_id"],
        signal_input_completeness_id=fixture["signal_input_completeness_id"],
        observation_dir=tmp_path / "paper_shadow_daily",
        drift_dir=tmp_path / "paper_shadow_drift_monitor",
        contract_dir=tmp_path / "formal_research_method_contract",
        ledger_dir=tmp_path / "candidate_decision_ledger",
        signal_input_completeness_dir=tmp_path / "signal_input_completeness",
        output_dir=tmp_path / "paper_shadow_weekly_review",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    review = result["paper_shadow_weekly_review"]

    assert result["paper_shadow_weekly_validation"]["status"] == "PASS"
    assert review["weekly_decision"] == "CONTINUE"
    assert review["selected_window_start"] == "2026-06-12"
    assert review["selected_window_end"] == "2026-06-12"
    assert review["coverage_classification"] == "RECOVERY_MODE_REVIEW"
    assert review["coverage_safe_for_continuation"] is False
    assert review["coverage_status"] == "MANUAL_REVIEW_REQUIRED"
    assert review["covered_market_days"] == ["2026-06-12"]
    assert review["missing_market_days"] == [
        "2026-06-08",
        "2026-06-09",
        "2026-06-10",
        "2026-06-11",
    ]
    assert review["coverage_ratio"] == 0.2
    assert "paper_shadow_weekly_coverage_classification" in result["reader_brief_section"]


def _weekly_fixture(
    tmp_path: Path,
    *,
    missing_market_panel: bool = False,
    days: tuple[str, ...] = (
        "2026-06-08",
        "2026-06-09",
        "2026-06-10",
        "2026-06-11",
        "2026-06-12",
    ),
) -> dict[str, Any]:
    fixture = run_paper_shadow_protocol_fixture(tmp_path)
    signal_input = run_signal_input_completeness_fixture(tmp_path, as_of="2026-06-12")
    ledger = _candidate_decision_ledger_fixture(tmp_path, fixture)
    daily_ids: list[str] = []
    drift_ids: list[str] = []
    for day in days:
        market_panel = tmp_path / f"market_panel_{day}.json"
        if not missing_market_panel or day != "2026-06-12":
            market_panel.write_text(
                json.dumps({"as_of": day, "report_type": "market_panel"}),
                encoding="utf-8",
            )
        signal_artifact = tmp_path / f"candidate_signal_summary_{day}.json"
        signal_artifact.write_text(
            json.dumps(
                {
                    "as_of": day,
                    "candidate": readiness.TOP_FILTERED_CANDIDATE,
                    "signal_output": "OBSERVE_RISK_ON",
                }
            ),
            encoding="utf-8",
        )
        observation = daily.run_paper_shadow_daily_observation(
            candidate=readiness.TOP_FILTERED_CANDIDATE,
            observation_date=day,
            market_panel_artifact=market_panel,
            signal_artifact=signal_artifact,
            signal_output="OBSERVE_RISK_ON",
            hypothetical_weight_recommendation="paper_shadow_only_no_official_weight",
            risk_off_risk_on_state="risk_on",
            drawdown_state="normal",
            rotation_event="none",
            mismatch_event="none",
            benchmark_comparison="tracking_QQQ_SPY_SMH",
            manual_reviewer_notes="synthetic weekly review fixture",
            contract_id=fixture["formal_research_method_contract"]["contract_id"],
            protocol_id=fixture["paper_shadow_protocol"]["protocol_id"],
            signal_input_completeness_id=signal_input["monitor_id"],
            contract_dir=tmp_path / "formal_research_method_contract",
            protocol_dir=tmp_path / "paper_shadow_protocol",
            signal_input_completeness_dir=tmp_path / "signal_input_completeness",
            output_dir=tmp_path / "paper_shadow_daily",
            generated_at=datetime(2026, 6, 15, tzinfo=UTC),
        )
        daily_ids.append(observation["observation_id"])
        monitor = drift.build_paper_shadow_drift_monitor_report(
            observation_id=observation["observation_id"],
            observation_dir=tmp_path / "paper_shadow_daily",
            contract_id=fixture["formal_research_method_contract"]["contract_id"],
            contract_dir=tmp_path / "formal_research_method_contract",
            output_dir=tmp_path / "paper_shadow_drift_monitor",
            generated_at=datetime(2026, 6, 15, tzinfo=UTC),
        )
        drift_ids.append(monitor["monitor_id"])
    return {
        "contract_id": fixture["formal_research_method_contract"]["contract_id"],
        "ledger_run_id": ledger["ledger_run_id"],
        "signal_input_completeness_id": signal_input["monitor_id"],
        "daily_ids": daily_ids,
        "drift_ids": drift_ids,
    }


def _candidate_decision_ledger_fixture(
    tmp_path: Path,
    fixture: dict[str, Any],
) -> dict[str, Any]:
    return readiness.record_candidate_decision_ledger(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        evidence_id=fixture["filtered_candidate_evidence"]["evidence_id"],
        stress_backfill_id=fixture["filtered_candidate_stress_backfill"][
            "stress_backfill_id"
        ],
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
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
