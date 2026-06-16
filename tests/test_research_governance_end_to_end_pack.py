from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.research_governance_end_to_end_pack import (
    GOVERNANCE_BLOCKED,
    GOVERNANCE_HEALTHY,
    PASS_STATUS,
    PASS_WITH_WARNINGS_STATUS,
    SOURCE_REPORT_SPECS,
    build_research_governance_end_to_end_pack_payload,
    validate_research_governance_end_to_end_pack_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_research_governance_end_to_end_pack_healthy(
    tmp_path: Path,
) -> None:
    payload = build_research_governance_end_to_end_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_research_governance_end_to_end_pack_payload(payload)

    assert payload["overall_governance_status"] == GOVERNANCE_HEALTHY
    assert payload["summary"]["source_report_count"] == len(SOURCE_REPORT_SPECS)
    assert payload["summary"]["blocking_item_count"] == 0
    assert payload["summary"]["warning_item_count"] == 0
    assert validation["validation_status"] == PASS_STATUS


def test_research_governance_end_to_end_pack_blocks_visible_governance_failures(
    tmp_path: Path,
) -> None:
    report_index = _report_index_payload(
        tmp_path,
        payload_overrides={
            "research_monthly_review_pack": {
                "monthly_review_status": "MONTHLY_REVIEW_BLOCKED",
                "next_action": "resolve_monthly_blockers",
            },
            "paper_shadow_promotion_board": {
                "board_decision": "HOLD_FOR_MORE_DATA",
                "next_action": "resolve_promotion_board_required_evidence",
            },
            "extended_shadow_protocol": {
                "eligibility_status": "EXTENDED_SHADOW_BLOCKED",
                "next_action": "resolve_extended_shadow_protocol_blockers",
            },
            "research_roadmap_dashboard": {
                "dashboard_status": "ROADMAP_BLOCKED",
                "next_action": "review_top_roadmap_blockers",
            },
        },
        validation_overrides={
            "research_monthly_review_pack_validation": {
                "validation_status": "PASS_WITH_WARNINGS",
            },
            "research_roadmap_dashboard_validation": {
                "validation_status": "PASS_WITH_WARNINGS",
            },
        },
    )

    payload = build_research_governance_end_to_end_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=report_index,
        project_root=tmp_path,
    )
    validation = validate_research_governance_end_to_end_pack_payload(payload)

    assert payload["overall_governance_status"] == GOVERNANCE_BLOCKED
    assert payload["summary"]["blocking_item_count"] >= 4
    assert any(
        blocker["source_id"] == "extended_shadow_protocol"
        for blocker in payload["top_blockers"]
    )
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_research_governance_end_to_end_pack_validation_fails_missing_source(
    tmp_path: Path,
) -> None:
    payload = build_research_governance_end_to_end_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(
            tmp_path,
            omit={"artifact_lineage_validation"},
        ),
        project_root=tmp_path,
    )
    validation = validate_research_governance_end_to_end_pack_payload(payload)

    assert payload["overall_governance_status"] == GOVERNANCE_BLOCKED
    assert validation["validation_status"] == "FAIL"
    assert any(
        issue["issue_id"] == "required_sources_available"
        for issue in validation["blocking_issues"]
    )


def test_research_governance_end_to_end_pack_cli_writes_report_and_validation(
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
            "research-governance-end-to-end-pack",
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
            "validate-research-governance-end-to-end-pack",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_payload = json.loads(
        (reports_dir / "research_governance_end_to_end_pack_2026-05-04.json").read_text(
            encoding="utf-8"
        )
    )
    validation_payload = json.loads(
        (
            reports_dir
            / "research_governance_end_to_end_pack_validation_2026-05-04.json"
        ).read_text(encoding="utf-8")
    )
    assert report_payload["overall_governance_status"] == GOVERNANCE_HEALTHY
    assert validation_payload["validation_status"] == PASS_STATUS
    assert validation_payload["input_artifacts"][
        "research_governance_end_to_end_pack"
    ].endswith("research_governance_end_to_end_pack_2026-05-04.json")


def test_reader_brief_research_governance_end_to_end_pack_summary(
    tmp_path: Path,
) -> None:
    payload = build_research_governance_end_to_end_pack_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index_payload(tmp_path),
        project_root=tmp_path,
    )
    validation = validate_research_governance_end_to_end_pack_payload(payload)
    report_path = tmp_path / "research_governance_end_to_end_pack_2026-05-04.json"
    validation_path = (
        tmp_path / "research_governance_end_to_end_pack_validation_2026-05-04.json"
    )
    report_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._research_governance_end_to_end_pack_summary(
        {
            "reports": [
                {
                    "report_id": "research_governance_end_to_end_pack",
                    "latest_artifact_path": str(report_path),
                },
                {
                    "report_id": "research_governance_end_to_end_pack_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["overall_governance_status"] == GOVERNANCE_HEALTHY
    assert summary["validation_status"] == PASS_STATUS
    assert summary["source_report_count"] == len(SOURCE_REPORT_SPECS)
    assert summary["production_effect"] == "none"


def _report_index_payload(
    tmp_path: Path,
    *,
    payload_overrides: dict[str, dict[str, object]] | None = None,
    validation_overrides: dict[str, dict[str, object]] | None = None,
    omit: set[str] | None = None,
) -> dict[str, object]:
    payload_overrides = payload_overrides or {}
    validation_overrides = validation_overrides or {}
    omit = omit or set()
    reports: list[dict[str, object]] = []
    for spec in SOURCE_REPORT_SPECS:
        report_id = str(spec["report_id"])
        validation_report_id = str(spec["validation_report_id"])
        if report_id not in omit:
            payload = _source_payload(report_id)
            payload.update(payload_overrides.get(report_id, {}))
            source_path = tmp_path / "sources" / report_id / f"{report_id}.json"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            reports.append(_index_entry(report_id, source_path))
        if validation_report_id not in omit:
            validation_payload = _validation_payload(validation_report_id)
            validation_payload.update(validation_overrides.get(validation_report_id, {}))
            validation_path = (
                tmp_path
                / "sources"
                / validation_report_id
                / f"{validation_report_id}.json"
            )
            validation_path.parent.mkdir(parents=True, exist_ok=True)
            validation_path.write_text(
                json.dumps(validation_payload, ensure_ascii=False),
                encoding="utf-8",
            )
            reports.append(_index_entry(validation_report_id, validation_path))
    return {
        "schema_version": 1,
        "report_type": "report_index",
        "as_of": RUN_DATE.isoformat(),
        "status": "PASS",
        "summary": {
            "report_count": len(reports),
            "missing_count": 0,
            "stale_count": 0,
            "required_missing_count": 0,
            "unwaived_warning_count": 0,
            "explicit_waiver_count": 0,
        },
        "reports": reports,
        "production_effect": "none",
    }


def _index_entry(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "artifact_status": "AVAILABLE",
        "freshness_status": "FRESH",
        "production_effect": "none",
    }


def _source_payload(report_id: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "report_type": report_id,
        "status": "PASS",
        "next_action": "continue_manual_governance_review",
        "production_effect": "none",
        "summary": {},
    }
    payload.update(
        {
            "research_safety_boundary_audit": {"safety_status": "SAFETY_PASS"},
            "production_boundary_static_scan": {"scan_status": "OK"},
            "owner_review_template_v2": {"template_status": "PASS"},
            "owner_decision_audit_log": {"audit_log_status": "AUDIT_LOG_PASS"},
            "research_monthly_review_pack": {"monthly_review_status": "MONTHLY_REVIEW_READY"},
            "paper_shadow_promotion_board": {"board_decision": "CONTINUE_NORMAL_SHADOW"},
            "extended_shadow_protocol": {"eligibility_status": "EXTENDED_SHADOW_ELIGIBLE"},
            "research_roadmap_dashboard": {"dashboard_status": "ROADMAP_HEALTHY"},
            "artifact_lineage_graph": {"lineage_status": "PASS"},
            "report_index_waiver_inventory": {"waiver_inventory_status": "PASS"},
            "reader_brief_consistency_pack": {"consistency_status": "PASS"},
            "task_register_consistency": {"consistency_status": "PASS"},
        }.get(report_id, {})
    )
    return payload


def _validation_payload(report_id: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": report_id,
        "validation_status": "PASS",
        "status": "PASS",
        "production_effect": "none",
        "summary": {"failed_check_count": 0},
    }
