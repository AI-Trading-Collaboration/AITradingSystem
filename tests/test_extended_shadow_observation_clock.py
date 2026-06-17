from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.extended_shadow_observation_clock import (
    OBSERVATION_PERIOD_MET,
    OBSERVATION_PERIOD_PARTIAL,
    OBSERVATION_PERIOD_UNMET,
    PASS_STATUS,
    PASS_WITH_WARNINGS_STATUS,
    SOURCE_SPECS,
    build_extended_shadow_observation_clock_payload,
    validate_extended_shadow_observation_clock_payload,
)
from ai_trading_system.reports.extended_shadow_protocol import (
    EXTENDED_SHADOW_NOT_READY,
    MINIMUM_OBSERVATION_TRADING_DAYS,
    build_extended_shadow_protocol_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_observation_clock_unmet_when_no_days_are_available(tmp_path: Path) -> None:
    payload = build_extended_shadow_observation_clock_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_extended_shadow_observation_clock_payload(payload)

    assert payload["observation_clock_status"] == OBSERVATION_PERIOD_UNMET
    assert payload["summary"]["current_count"] == 0
    assert payload["summary"]["required_count"] == MINIMUM_OBSERVATION_TRADING_DAYS
    assert "minimum_observation_period_unmet_0_of_20" in payload["observation_window"][
        "invalid_reasons"
    ]
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_observation_clock_partial_from_weekly_review_count(tmp_path: Path) -> None:
    payload = build_extended_shadow_observation_clock_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(
            tmp_path,
            overrides={
                "etf_dynamic_v3_paper_shadow_weekly_review": {
                    "summary": {"observation_trading_days": 5},
                },
            },
        ),
        project_root=tmp_path,
    )

    assert payload["observation_clock_status"] == OBSERVATION_PERIOD_PARTIAL
    assert payload["summary"]["current_count"] == 5
    assert payload["summary"]["missing_day_count"] == 15


def test_observation_clock_met_from_complete_day_list(tmp_path: Path) -> None:
    days = [f"2026-05-{day:02d}" for day in range(1, 21)]
    payload = build_extended_shadow_observation_clock_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(
            tmp_path,
            overrides={
                "etf_dynamic_v3_paper_shadow_weekly_review": {
                    "summary": {
                        "observation_start_date": days[0],
                        "complete_observation_trading_days": days,
                    },
                },
            },
        ),
        project_root=tmp_path,
    )
    validation = validate_extended_shadow_observation_clock_payload(payload)

    assert payload["observation_clock_status"] == OBSERVATION_PERIOD_MET
    assert payload["summary"]["current_count"] == MINIMUM_OBSERVATION_TRADING_DAYS
    assert payload["summary"]["observation_start_date"] == "2026-05-01"
    assert validation["validation_status"] == PASS_STATUS


def test_observation_clock_cli_writes_report_and_validation(tmp_path: Path) -> None:
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
            "extended-shadow-observation-clock",
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
            "validate-extended-shadow-observation-clock",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_payload = json.loads(
        (reports_dir / "extended_shadow_observation_clock_2026-05-04.json").read_text(
            encoding="utf-8"
        )
    )
    validation_payload = json.loads(
        (
            reports_dir / "extended_shadow_observation_clock_validation_2026-05-04.json"
        ).read_text(encoding="utf-8")
    )
    assert report_payload["observation_clock_status"] == OBSERVATION_PERIOD_UNMET
    assert validation_payload["validation_status"] == PASS_WITH_WARNINGS_STATUS
    assert validation_payload["input_artifacts"]["extended_shadow_observation_clock"].endswith(
        "extended_shadow_observation_clock_2026-05-04.json"
    )


def test_reader_brief_observation_clock_summary(tmp_path: Path) -> None:
    payload = build_extended_shadow_observation_clock_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_extended_shadow_observation_clock_payload(payload)
    report_path = tmp_path / "extended_shadow_observation_clock_2026-05-04.json"
    validation_path = tmp_path / "extended_shadow_observation_clock_validation_2026-05-04.json"
    report_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._extended_shadow_observation_clock_summary(
        {
            "reports": [
                {
                    "report_id": "extended_shadow_observation_clock",
                    "latest_artifact_path": str(report_path),
                },
                {
                    "report_id": "extended_shadow_observation_clock_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["observation_clock_status"] == OBSERVATION_PERIOD_UNMET
    assert summary["validation_status"] == PASS_WITH_WARNINGS_STATUS
    assert summary["current_count"] == 0
    assert summary["production_effect"] == "none"


def test_extended_shadow_protocol_uses_observation_clock_when_available(
    tmp_path: Path,
) -> None:
    clock_payload = build_extended_shadow_observation_clock_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(
            tmp_path,
            overrides={
                "etf_dynamic_v3_paper_shadow_weekly_review": {
                    "summary": {"observation_trading_days": 5},
                },
            },
        ),
        project_root=tmp_path,
    )
    protocol_index = _extended_protocol_report_index_payload(
        tmp_path,
        clock_payload=clock_payload,
        weekly_observation_count=MINIMUM_OBSERVATION_TRADING_DAYS,
    )

    protocol = build_extended_shadow_protocol_payload(
        as_of=RUN_DATE,
        report_index_payload=protocol_index,
        project_root=tmp_path,
    )

    assert protocol["eligibility_status"] == EXTENDED_SHADOW_NOT_READY
    assert protocol["summary"]["observed_trading_days"] == 5
    assert any(
        check["source_id"] == "observation_clock" and check["check_status"] == "WARNING"
        for check in protocol["eligibility_checklist"]
    )
    assert any(
        reason["source_id"] == "minimum_observation_period"
        for reason in protocol["blocking_reasons"]
    )


def _report_index_payload(
    tmp_path: Path,
    *,
    overrides: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    overrides = overrides or {}
    reports: list[dict[str, object]] = []
    for spec in SOURCE_SPECS:
        report_id = str(spec["report_id"])
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


def _extended_protocol_report_index_payload(
    tmp_path: Path,
    *,
    clock_payload: dict[str, object],
    weekly_observation_count: int,
) -> dict[str, object]:
    from ai_trading_system.reports.extended_shadow_protocol import CHECK_SPECS

    reports: list[dict[str, object]] = []
    for spec in CHECK_SPECS:
        report_id = str(spec["report_id"])
        if report_id == "extended_shadow_observation_clock":
            payload = clock_payload
        else:
            payload = _protocol_source_payload(report_id, weekly_observation_count)
        source_path = tmp_path / "protocol_sources" / report_id / f"{report_id}.json"
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
        "next_action": "continue_collecting_valid_observation_days",
        "production_effect": "none",
        "summary": {},
    }
    payload.update(
        {
            "paper_shadow_promotion_board": {
                "board_decision": "HOLD_FOR_MORE_DATA",
            },
            "etf_dynamic_v3_paper_shadow_weekly_review": {
                "coverage_status": "FULL_WEEK_REVIEW",
                "weekly_decision": "CONTINUE",
            },
        }.get(report_id, {})
    )
    return payload


def _protocol_source_payload(report_id: str, weekly_observation_count: int) -> dict[str, object]:
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
            "paper_shadow_promotion_board": {"board_decision": "EXTEND_SHADOW"},
            "etf_dynamic_v3_evidence_staleness_monitor": {
                "evidence_freshness_status": "ACCEPTABLE",
            },
            "etf_dynamic_v3_paper_shadow_weekly_review": {
                "coverage_status": "FULL_WEEK_REVIEW",
                "weekly_decision": "CONTINUE",
                "summary": {"observation_trading_days": weekly_observation_count},
            },
            "etf_dynamic_v3_shadow_continuation_readiness": {
                "shadow_continuation_readiness": "READY_TO_CONTINUE",
            },
            "research_safety_boundary_audit": {"safety_status": "SAFETY_PASS"},
            "etf_dynamic_v3_cost_sensitivity_review": {
                "cost_sensitivity_status": "COST_REVIEW_PASS",
            },
            "etf_dynamic_v3_benchmark_baseline_control": {
                "benchmark_baseline_status": "BASELINE_CONTROL_PASS",
            },
            "owner_decision_audit_log": {"audit_log_status": "AUDIT_LOG_PASS"},
            "artifact_lineage_graph": {"lineage_status": "PASS"},
        }.get(report_id, {})
    )
    return payload
