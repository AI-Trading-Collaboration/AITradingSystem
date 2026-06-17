from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.extended_shadow_protocol import (
    CHECK_SPECS,
    EXTENDED_SHADOW_BLOCKED,
    EXTENDED_SHADOW_ELIGIBLE,
    MINIMUM_OBSERVATION_TRADING_DAYS,
    PASS_STATUS,
    PASS_WITH_WARNINGS_STATUS,
    build_extended_shadow_protocol_payload,
    validate_extended_shadow_protocol_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_extended_shadow_protocol_eligible_with_strict_evidence(tmp_path: Path) -> None:
    payload = build_extended_shadow_protocol_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_extended_shadow_protocol_payload(payload)

    assert payload["eligibility_status"] == EXTENDED_SHADOW_ELIGIBLE
    assert payload["summary"]["observed_trading_days"] == MINIMUM_OBSERVATION_TRADING_DAYS
    assert payload["summary"]["blocked_check_count"] == 0
    assert validation["validation_status"] == PASS_STATUS


def test_extended_shadow_protocol_blocks_unresolved_warnings_and_observation_gap(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        overrides={
            "paper_shadow_promotion_board": {"board_decision": "HOLD_FOR_MORE_DATA"},
            "research_safety_boundary_audit": {"safety_status": "SAFETY_PASS_WITH_WARNINGS"},
            "owner_decision_audit_log": {"audit_log_status": "AUDIT_LOG_EMPTY"},
            "etf_dynamic_v3_paper_shadow_weekly_review": {
                "summary": {"observation_trading_days": 5},
            },
            "extended_shadow_observation_clock": {
                "observation_clock_status": "OBSERVATION_PERIOD_PARTIAL",
                "summary": {
                    "current_count": 5,
                    "required_count": MINIMUM_OBSERVATION_TRADING_DAYS,
                },
            },
        },
    )

    payload = build_extended_shadow_protocol_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_extended_shadow_protocol_payload(payload)

    assert payload["eligibility_status"] == EXTENDED_SHADOW_BLOCKED
    assert payload["summary"]["observed_trading_days"] == 5
    assert payload["summary"]["blocked_check_count"] >= 3
    assert any(
        reason["source_id"] == "minimum_observation_period"
        for reason in payload["blocking_reasons"]
    )
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_extended_shadow_protocol_validation_fails_missing_required_source(
    tmp_path: Path,
) -> None:
    payload = build_extended_shadow_protocol_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path, omit={"artifact_lineage_graph"}),
        project_root=tmp_path,
    )
    validation = validate_extended_shadow_protocol_payload(payload)

    assert payload["eligibility_status"] == EXTENDED_SHADOW_BLOCKED
    assert validation["validation_status"] == "FAIL"
    assert any(
        issue["issue_id"] == "required_source_artifacts_available"
        for issue in validation["blocking_issues"]
    )


def test_extended_shadow_protocol_cli_writes_report_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    report_index = _report_index_payload(tmp_path)
    report_index_path = reports_dir / "report_index_2026-05-04.json"
    report_index_path.parent.mkdir(parents=True, exist_ok=True)
    report_index_path.write_text(json.dumps(report_index, ensure_ascii=False), encoding="utf-8")
    runner = CliRunner()

    report_result = runner.invoke(
        app,
        [
            "reports",
            "extended-shadow-protocol",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--report-index-path",
            str(report_index_path),
            "--project-root",
            str(tmp_path),
        ],
    )
    assert report_result.exit_code == 0, report_result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-extended-shadow-protocol",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_payload = json.loads(
        (reports_dir / "extended_shadow_protocol_2026-05-04.json").read_text(
            encoding="utf-8"
        )
    )
    validation_payload = json.loads(
        (reports_dir / "extended_shadow_protocol_validation_2026-05-04.json").read_text(
            encoding="utf-8"
        )
    )
    assert report_payload["eligibility_status"] == EXTENDED_SHADOW_ELIGIBLE
    assert validation_payload["validation_status"] == PASS_STATUS
    assert validation_payload["input_artifacts"]["extended_shadow_protocol"].endswith(
        "extended_shadow_protocol_2026-05-04.json"
    )


def test_reader_brief_extended_shadow_protocol_summary(tmp_path: Path) -> None:
    payload = build_extended_shadow_protocol_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_extended_shadow_protocol_payload(payload)
    report_path = tmp_path / "extended_shadow_protocol_2026-05-04.json"
    validation_path = tmp_path / "extended_shadow_protocol_validation_2026-05-04.json"
    report_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._extended_shadow_protocol_summary(
        {
            "reports": [
                {
                    "report_id": "extended_shadow_protocol",
                    "latest_artifact_path": str(report_path),
                },
                {
                    "report_id": "extended_shadow_protocol_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["eligibility_status"] == EXTENDED_SHADOW_ELIGIBLE
    assert summary["validation_status"] == PASS_STATUS
    assert summary["minimum_observation_trading_days"] == MINIMUM_OBSERVATION_TRADING_DAYS
    assert summary["production_effect"] == "none"


def _report_index_payload(
    tmp_path: Path,
    *,
    overrides: dict[str, dict[str, object]] | None = None,
    omit: set[str] | None = None,
) -> dict[str, object]:
    overrides = overrides or {}
    omit = omit or set()
    reports: list[dict[str, object]] = []
    for spec in CHECK_SPECS:
        report_id = str(spec["report_id"])
        if report_id in omit:
            continue
        payload = _source_payload(report_id)
        payload.update(overrides.get(report_id, {}))
        source_path = tmp_path / "sources" / report_id / f"{report_id}.json"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        reports.append(
            {
                "report_id": report_id,
                "latest_artifact_path": str(source_path),
                "artifact_status": "AVAILABLE",
                "freshness_status": "FRESH",
                "production_effect": "none",
            }
        )
    return {
        "schema_version": 1,
        "report_type": "report_index",
        "as_of": RUN_DATE.isoformat(),
        "reports": reports,
        "production_effect": "none",
    }


def _source_payload(report_id: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "report_type": report_id,
        "candidate": "candidate_a",
        "status": "PASS",
        "next_action": "continue_manual_extended_shadow_review",
        "production_effect": "none",
        "summary": {},
    }
    payload.update(
        {
            "paper_shadow_promotion_board": {
                "board_decision": "EXTEND_SHADOW",
            },
            "etf_dynamic_v3_evidence_staleness_monitor": {
                "evidence_freshness_status": "ACCEPTABLE",
            },
            "etf_dynamic_v3_paper_shadow_weekly_review": {
                "coverage_status": "FULL_WEEK_REVIEW",
                "weekly_decision": "CONTINUE",
                "summary": {"observation_trading_days": MINIMUM_OBSERVATION_TRADING_DAYS},
            },
            "etf_dynamic_v3_shadow_continuation_readiness": {
                "shadow_continuation_readiness": "READY_TO_CONTINUE",
            },
            "research_safety_boundary_audit": {
                "safety_status": "SAFETY_PASS",
            },
            "etf_dynamic_v3_cost_sensitivity_review": {
                "cost_sensitivity_status": "COST_REVIEW_PASS",
            },
            "etf_dynamic_v3_benchmark_baseline_control": {
                "benchmark_baseline_status": "BASELINE_CONTROL_PASS",
            },
            "owner_decision_audit_log": {
                "audit_log_status": "AUDIT_LOG_PASS",
            },
            "artifact_lineage_graph": {
                "lineage_status": "PASS",
            },
            "extended_shadow_observation_clock": {
                "observation_clock_status": "OBSERVATION_PERIOD_MET",
                "summary": {
                    "current_count": MINIMUM_OBSERVATION_TRADING_DAYS,
                    "required_count": MINIMUM_OBSERVATION_TRADING_DAYS,
                },
            },
        }.get(report_id, {})
    )
    return payload
