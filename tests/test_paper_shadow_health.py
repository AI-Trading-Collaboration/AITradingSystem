from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_signal_input_completeness_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as health
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st


def test_paper_shadow_health_blocks_signal_input_fail_closed(tmp_path: Path) -> None:
    fixture = _paper_shadow_health_fixture(tmp_path, signal_status="BLOCKING")

    result = health.run_paper_shadow_health_report(
        as_of=date(2024, 4, 22),
        price_cache_path=fixture["price_cache_path"],
        market_panel_dir=fixture["market_panel_dir"],
        signal_input_completeness_id=fixture["signal_input_id"],
        paper_shadow_daily_id=fixture["daily_id"],
        paper_shadow_drift_monitor_id=fixture["drift_id"],
        paper_shadow_weekly_review_id=fixture["weekly_id"],
        evidence_staleness_monitor_id=fixture["staleness_id"],
        shadow_continuation_readiness_id=fixture["readiness_id"],
        fallback_policy_report_path=fixture["fallback_report_path"],
        cache_catalog_report_path=fixture["cache_catalog_report_path"],
        data_refresh_audit_id=fixture["data_refresh_audit_id"],
        signal_input_completeness_dir=fixture["signal_input_dir"],
        paper_shadow_daily_dir=fixture["daily_dir"],
        paper_shadow_drift_monitor_dir=fixture["drift_dir"],
        paper_shadow_weekly_review_dir=fixture["weekly_dir"],
        evidence_staleness_monitor_dir=fixture["staleness_dir"],
        shadow_continuation_readiness_dir=fixture["readiness_dir"],
        data_refresh_audit_dir=fixture["data_refresh_audit_dir"],
        output_dir=fixture["health_dir"],
        generated_at=datetime(2024, 4, 22, 2, tzinfo=UTC),
    )
    report = result["paper_shadow_health_report"]
    validation = result["paper_shadow_health_validation"]
    payload = health.paper_shadow_health_report_payload(
        health_id=result["health_id"],
        output_dir=fixture["health_dir"],
    )

    assert report["paper_shadow_health_status"] == "BLOCKED_SIGNAL_INPUTS"
    assert report["safe_to_continue_shadow"] is False
    assert report["signal_input_status"] == "BLOCKING"
    assert report["blocking_reasons"] == ["signal_input_completeness:blocking"]
    assert report["next_required_action"] == "stop_paper_shadow_until_signal_inputs_are_restored"
    assert report["source_artifacts"]["signal_input_completeness"]["status"] == "BLOCKING"
    assert validation["status"] == "PASS"
    assert payload["paper_shadow_health_report"]["health_id"] == result["health_id"]
    assert "paper_shadow_health_status" in result["reader_brief_section"]
    assert_research_safe(result["manifest"])


def test_paper_shadow_health_cli_run_report_and_validate(tmp_path: Path) -> None:
    fixture = _paper_shadow_health_fixture(tmp_path, signal_status="OK")
    output_dir = tmp_path / "paper_shadow_health_cli"

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "paper-shadow-health",
            "run",
            "--as-of",
            "2024-04-22",
            "--price-cache-path",
            str(fixture["price_cache_path"]),
            "--market-panel-dir",
            str(fixture["market_panel_dir"]),
            "--signal-input-completeness-id",
            fixture["signal_input_id"],
            "--paper-shadow-daily-id",
            fixture["daily_id"],
            "--paper-shadow-drift-monitor-id",
            fixture["drift_id"],
            "--paper-shadow-weekly-review-id",
            fixture["weekly_id"],
            "--evidence-staleness-monitor-id",
            fixture["staleness_id"],
            "--shadow-continuation-readiness-id",
            fixture["readiness_id"],
            "--fallback-policy-report-path",
            str(fixture["fallback_report_path"]),
            "--cache-catalog-report-path",
            str(fixture["cache_catalog_report_path"]),
            "--data-refresh-audit-id",
            fixture["data_refresh_audit_id"],
            "--signal-input-completeness-dir",
            str(fixture["signal_input_dir"]),
            "--paper-shadow-daily-dir",
            str(fixture["daily_dir"]),
            "--paper-shadow-drift-monitor-dir",
            str(fixture["drift_dir"]),
            "--paper-shadow-weekly-review-dir",
            str(fixture["weekly_dir"]),
            "--evidence-staleness-monitor-dir",
            str(fixture["staleness_dir"]),
            "--shadow-continuation-readiness-dir",
            str(fixture["readiness_dir"]),
            "--data-refresh-audit-dir",
            str(fixture["data_refresh_audit_dir"]),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    assert "paper_shadow_health_status=HEALTHY" in run.output
    assert "safe_to_continue_shadow=True" in run.output
    assert "validation_status=PASS" in run.output
    health_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("health_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "paper-shadow-health",
            "report",
            "--health-id",
            health_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "paper_shadow_health_status=HEALTHY" in report.output
    assert "blocking_reasons=" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-paper-shadow-health",
            "--health-id",
            health_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def _paper_shadow_health_fixture(
    tmp_path: Path,
    *,
    signal_status: str,
) -> dict[str, Any]:
    signal_input = run_signal_input_completeness_fixture(
        tmp_path,
        as_of="2024-04-22",
        missing_signal_series=signal_status == "BLOCKING",
    )
    fixture = {
        "price_cache_path": _write_price_cache(tmp_path),
        "market_panel_dir": _write_market_panel(tmp_path),
        "signal_input_dir": tmp_path / "signal_input_completeness",
        "daily_dir": tmp_path / "paper_shadow_daily",
        "drift_dir": tmp_path / "paper_shadow_drift_monitor",
        "weekly_dir": tmp_path / "paper_shadow_weekly_review",
        "staleness_dir": tmp_path / "evidence_staleness_monitor",
        "readiness_dir": tmp_path / "shadow_continuation_readiness",
        "data_refresh_audit_dir": tmp_path / "data_refresh_audit",
        "health_dir": tmp_path / "paper_shadow_health",
        "signal_input_id": signal_input["monitor_id"],
        "daily_id": "paper-shadow-daily-test",
        "drift_id": "paper-shadow-drift-test",
        "weekly_id": "paper-shadow-weekly-test",
        "staleness_id": "evidence-staleness-test",
        "readiness_id": "shadow-readiness-test",
        "data_refresh_audit_id": "data-refresh-audit-test",
    }
    _write_daily_artifact(fixture["daily_dir"], fixture["daily_id"])
    _write_drift_artifact(fixture["drift_dir"], fixture["drift_id"])
    _write_weekly_artifact(fixture["weekly_dir"], fixture["weekly_id"])
    _write_staleness_artifact(fixture["staleness_dir"], fixture["staleness_id"])
    _write_readiness_artifact(fixture["readiness_dir"], fixture["readiness_id"])
    fixture["fallback_report_path"] = _write_fallback_policy_report(tmp_path)
    fixture["cache_catalog_report_path"] = _write_cache_catalog_report(tmp_path)
    _write_data_refresh_audit(
        fixture["data_refresh_audit_dir"],
        fixture["data_refresh_audit_id"],
    )
    return fixture


def _write_price_cache(tmp_path: Path) -> Path:
    path = tmp_path / "prices_daily.csv"
    path.write_text("date,ticker,close\n2024-04-22,QQQ,431\n", encoding="utf-8")
    return path


def _write_market_panel(tmp_path: Path) -> Path:
    root = tmp_path / "market_panel"
    root.mkdir()
    _write_json(
        root / "market_panel_2024-04-22.json",
        {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "market_panel",
            "as_of": "2024-04-22",
            "status": "PASS",
            "validation_status": "PASS",
            "proxies": [{"ticker": "QQQ"}],
            "summary": {"coverage_status": "OK"},
            "production_effect": "none",
        },
    )
    return root


def _write_daily_artifact(root: Path, observation_id: str) -> None:
    artifact_dir = root / observation_id
    artifact_dir.mkdir(parents=True)
    observation_path = artifact_dir / "paper_shadow_daily_observation.json"
    observation = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_daily_observation",
        "observation_id": observation_id,
        "candidate": "filtered_dynamic_v3_candidate",
        "observation_date": "2024-04-22",
        "observation_status": "RECORDED",
        "signal_input_status": "OK",
        "daily_review": {"signal_output": "OBSERVE_RISK_ON"},
        "next_required_action": "continue_daily_paper_shadow_observation",
        "production_effect": "none",
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_daily_manifest",
        "observation_id": observation_id,
        "status": "PASS",
        "observation_status": "RECORDED",
        "paper_shadow_daily_observation_path": str(observation_path),
        "paper_shadow_daily_report_path": str(artifact_dir / "paper_shadow_daily_report.md"),
        "production_effect": "none",
    }
    _write_json(artifact_dir / "paper_shadow_daily_manifest.json", manifest)
    _write_json(observation_path, observation)
    _write_text(artifact_dir / "paper_shadow_daily_report.md", "# Daily\n")
    _write_text(artifact_dir / "reader_brief_section.md", "paper_shadow_daily_status: RECORDED\n")
    _write_json(
        artifact_dir / "paper_shadow_daily_validation.json",
        _validation_payload(observation_id),
    )


def _write_drift_artifact(root: Path, monitor_id: str) -> None:
    artifact_dir = root / monitor_id
    artifact_dir.mkdir(parents=True)
    report_path = artifact_dir / "paper_shadow_drift_report.json"
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_drift_report",
        "monitor_id": monitor_id,
        "candidate": "filtered_dynamic_v3_candidate",
        "observation_id": "paper-shadow-daily-test",
        "drift_severity": "NONE",
        "blocking_count": 0,
        "warning_count": 0,
        "next_action": "continue_weekly_shadow_review",
        "production_effect": "none",
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_drift_manifest",
        "monitor_id": monitor_id,
        "status": "PASS",
        "drift_severity": "NONE",
        "paper_shadow_drift_report_path": str(report_path),
        "production_effect": "none",
    }
    _write_json(artifact_dir / "paper_shadow_drift_manifest.json", manifest)
    _write_json(report_path, report)
    _write_jsonl(artifact_dir / "paper_shadow_drift_findings.jsonl", [])
    _write_text(artifact_dir / "paper_shadow_drift_report.md", "# Drift\n")
    _write_text(artifact_dir / "reader_brief_section.md", "drift_severity: NONE\n")
    _write_json(
        artifact_dir / "paper_shadow_drift_validation.json",
        _validation_payload(monitor_id),
    )


def _write_weekly_artifact(root: Path, weekly_id: str) -> None:
    artifact_dir = root / weekly_id
    artifact_dir.mkdir(parents=True)
    review_path = artifact_dir / "paper_shadow_weekly_review.json"
    review = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_review",
        "weekly_review_id": weekly_id,
        "candidate": "filtered_dynamic_v3_candidate",
        "weekly_decision": "CONTINUE",
        "coverage_classification": "FULL_WEEK_REVIEW",
        "coverage_status": "PASS",
        "coverage_safe_for_continuation": True,
        "summary": {"missing_input_artifacts": []},
        "production_effect": "none",
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_manifest",
        "weekly_review_id": weekly_id,
        "status": "PASS",
        "weekly_decision": "CONTINUE",
        "paper_shadow_weekly_report_path": str(artifact_dir / "paper_shadow_weekly_report.md"),
        "production_effect": "none",
    }
    _write_json(artifact_dir / "paper_shadow_weekly_manifest.json", manifest)
    _write_json(review_path, review)
    _write_text(artifact_dir / "paper_shadow_weekly_report.md", "# Weekly\n")
    _write_text(
        artifact_dir / "reader_brief_section.md",
        "paper_shadow_weekly_decision: CONTINUE\n",
    )
    _write_json(
        artifact_dir / "paper_shadow_weekly_validation.json",
        _validation_payload(weekly_id),
    )


def _write_staleness_artifact(root: Path, monitor_id: str) -> None:
    artifact_dir = root / monitor_id
    artifact_dir.mkdir(parents=True)
    report_path = artifact_dir / "evidence_staleness_report.json"
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_staleness_report",
        "monitor_id": monitor_id,
        "evidence_freshness_status": "FRESH",
        "blocking_artifacts": [],
        "stale_artifacts": [],
        "missing_artifacts": [],
        "next_refresh_action": "continue_shadow_review",
        "safe_to_continue_shadow": True,
        "safety_boundary_status": "PASS",
        "production_effect": "none",
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_staleness_manifest",
        "monitor_id": monitor_id,
        "status": "PASS",
        "evidence_freshness_status": "FRESH",
        "evidence_staleness_report_path": str(report_path),
        "production_effect": "none",
    }
    _write_json(artifact_dir / "evidence_staleness_manifest.json", manifest)
    _write_json(report_path, report)
    _write_jsonl(artifact_dir / "evidence_staleness_findings.jsonl", [])
    _write_text(artifact_dir / "evidence_staleness_report.md", "# Staleness\n")
    _write_text(artifact_dir / "reader_brief_section.md", "evidence_freshness_status: FRESH\n")
    _write_json(
        artifact_dir / "evidence_staleness_validation.json",
        _validation_payload(monitor_id),
    )


def _write_readiness_artifact(root: Path, readiness_id: str) -> None:
    artifact_dir = root / readiness_id
    artifact_dir.mkdir(parents=True)
    report_path = artifact_dir / "shadow_continuation_readiness_report.json"
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_continuation_readiness_report",
        "readiness_id": readiness_id,
        "shadow_continuation_readiness": "READY_TO_CONTINUE",
        "safe_to_continue_shadow": True,
        "missing_artifacts": [],
        "blocking_artifacts": [],
        "stale_artifacts": [],
        "coverage_status": "PASS",
        "manual_review_required": False,
        "next_required_action": "continue_paper_shadow_observation",
        "data_validation_status": "PASS",
        "safety_boundary_status": "PASS",
        "production_effect": "none",
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_continuation_readiness_manifest",
        "readiness_id": readiness_id,
        "status": "PASS",
        "shadow_continuation_readiness": "READY_TO_CONTINUE",
        "shadow_continuation_readiness_report_path": str(report_path),
        "production_effect": "none",
    }
    _write_json(artifact_dir / "shadow_continuation_readiness_manifest.json", manifest)
    _write_json(report_path, report)
    _write_text(artifact_dir / "shadow_continuation_readiness_report.md", "# Readiness\n")
    _write_text(
        artifact_dir / "reader_brief_section.md",
        "shadow_continuation_readiness: READY_TO_CONTINUE\n",
    )
    _write_json(
        artifact_dir / "shadow_continuation_readiness_validation.json",
        _validation_payload(readiness_id),
    )


def _write_fallback_policy_report(tmp_path: Path) -> Path:
    path = tmp_path / "data_source_fallback_policy.json"
    _write_json(
        path,
        {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "data_source_fallback_policy",
            "report_id": "fallback-policy-test",
            "as_of": "2024-04-22",
            "status": "PASS",
            "validation_status": "PASS",
            "production_effect": "none",
            "summary": {
                "fallback_status": "PRIMARY_OK",
                "source_group_count": 1,
                "primary_ok_count": 1,
                "fallback_used_count": 0,
                "fallback_unavailable_count": 0,
                "blocked_no_valid_source_count": 0,
                "blocking_source_count": 0,
                "fallback_used_sources": [],
                "blocking_data_types": [],
                "next_action": "continue_with_primary_sources",
            },
        },
    )
    return path


def _write_cache_catalog_report(tmp_path: Path) -> Path:
    path = tmp_path / "cache_catalog.json"
    _write_json(
        path,
        {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "cache_catalog",
            "catalog_id": "cache-catalog-test",
            "as_of": "2024-04-22",
            "status": "PASS",
            "validation_status": "PASS",
            "cache_integrity_status": "OK",
            "production_effect": "none",
            "summary": {
                "cache_integrity_status": "OK",
                "entry_count": 2,
                "required_entry_count": 2,
                "missing_required_count": 0,
                "missing_optional_count": 0,
                "checksum_mismatch_count": 0,
                "checksum_changed_without_refresh_count": 0,
                "blocking_entry_count": 0,
                "blocking_entry_ids": [],
                "warning_entry_ids": [],
                "refresh_audit_id": "data-refresh-audit-test",
                "validated_at": "2024-04-22T01:30:00+00:00",
                "next_action": "continue_with_current_cache",
            },
        },
    )
    return path


def _write_data_refresh_audit(root: Path, audit_id: str) -> None:
    artifact_dir = root / audit_id
    artifact_dir.mkdir(parents=True)
    _write_json(
        artifact_dir / "data_refresh_audit.json",
        {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "data_refresh_audit",
            "audit_id": audit_id,
            "as_of": "2024-04-22",
            "status": "SUCCESS",
            "validation_status": "PASS",
            "production_effect": "none",
            "summary": {
                "failed_record_count": 0,
                "warning_count": 0,
                "next_action": "continue_with_validated_data",
            },
        },
    )


def _validation_payload(artifact_id: str) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "test_validation",
        "artifact_id": artifact_id,
        "status": "PASS",
        "checks": [],
        "failed_check_count": 0,
        "production_effect": "none",
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
